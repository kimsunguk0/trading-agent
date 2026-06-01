"""Idempotency and duplicate-blocking helpers."""

from __future__ import annotations

import hashlib
import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, TypedDict

from core.models.market import Side
from core.models.order import OrderRequest

try:
    import asyncpg
except Exception:  # pragma: no cover - optional in local unit-only environments
    asyncpg = None

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None


logger = logging.getLogger(__name__)
_SCHEMA_RE = re.compile(r"^trading_(paper|live)$")


class IdempotencyPersistenceError(RuntimeError):
    """Raised when configured idempotency persistence cannot be updated."""


class _RequestRecord(TypedDict):
    order_intent_id: str
    account_id: str
    symbol: str
    side: Side
    unknown_submitted: bool
    status: str
    created_at: datetime


def make_idempotency_key(order_intent_id: str) -> str:
    return hashlib.sha256(order_intent_id.encode()).hexdigest()[:24]


class OrderIdempotencyManager:
    """Idempotency and UNKNOWN_SUBMITTED duplicate blocking."""

    ACTIVE_STATUSES = frozenset({"SUBMITTED", "UNKNOWN_SUBMITTED"})

    def __init__(
        self,
        *,
        dsn: str | None = None,
        schema: str | None = None,
        redis_url: str | None = None,
        redis_prefix: str | None = None,
    ) -> None:
        self._by_intent: dict[str, str] = {}
        self._by_block_key: dict[str, set[str]] = {}
        self._states: dict[str, _RequestRecord] = {}
        self.dsn = dsn
        self.schema = self._validate_schema(schema or f"trading_{os.getenv('ENVIRONMENT', 'paper').lower()}")
        self.redis_url = redis_url
        self.redis_prefix = redis_prefix or os.getenv("REDIS_STREAM_PREFIX", "paper.events")
        self._redis_failed = False
        self._lock = asyncio.Lock()
        self._finalized_intents: set[str] = set()

    def _block_key(self, request: OrderRequest) -> str:
        return f"{request.account_id}:{request.symbol}:{request.side}"

    def _block_key_from_record(self, record: _RequestRecord) -> str:
        return f"{record['account_id']}:{record['symbol']}:{record['side']}"

    @staticmethod
    def _validate_schema(schema: str) -> str:
        if _SCHEMA_RE.fullmatch(schema) is None:
            raise ValueError("schema must be trading_paper or trading_live")
        return schema

    def _remember(
        self,
        *,
        key: str,
        order_intent_id: str,
        account_id: str,
        symbol: str,
        side: str,
        status: str,
        created_at: datetime | None = None,
    ) -> None:
        record = _RequestRecord(
            order_intent_id=order_intent_id,
            account_id=account_id,
            symbol=symbol,
            side=Side(side),
            unknown_submitted=status == "UNKNOWN_SUBMITTED",
            status=status,
            created_at=created_at or datetime.now(timezone.utc),
        )
        self._by_intent[order_intent_id] = key
        self._states[key] = record
        if status in self.ACTIVE_STATUSES:
            self._by_block_key.setdefault(self._block_key_from_record(record), set()).add(key)
        else:
            self._finalized_intents.add(order_intent_id)

    def _forget(self, order_intent_id: str) -> str | None:
        key = self._by_intent.get(order_intent_id)
        if key is None:
            return None
        record = self._states.get(key)
        if record is not None:
            block_key = self._block_key_from_record(record)
            keys = self._by_block_key.get(block_key)
            if keys is not None:
                keys.discard(key)
                if not keys:
                    self._by_block_key.pop(block_key, None)
        self._states.pop(key, None)
        self._by_intent.pop(order_intent_id, None)
        return key

    def can_submit(self, request: OrderRequest) -> bool:
        key = self._by_intent.get(request.order_intent_id)
        if key is not None and key in self._states:
            return False
        return self._block_key(request) not in self._by_block_key

    def reserve(self, request: OrderRequest) -> str:
        key = make_idempotency_key(request.order_intent_id)
        self._remember(
            key=key,
            order_intent_id=request.order_intent_id,
            account_id=request.account_id,
            symbol=request.symbol,
            side=request.side,
            status="SUBMITTED",
        )
        return key

    def mark_unknown_submitted(self, request: OrderRequest) -> str:
        key = self._by_intent.get(request.order_intent_id) or self.reserve(request)
        state = self._states[key]
        state["unknown_submitted"] = True
        state["status"] = "UNKNOWN_SUBMITTED"
        self._by_block_key.setdefault(self._block_key(request), set()).add(key)
        return key

    def mark_finalized(self, request: OrderRequest) -> None:
        self._forget(request.order_intent_id)
        self._finalized_intents.add(request.order_intent_id)

    async def load(self) -> None:
        """Load active idempotency records from DB, or Redis when DB is absent."""

        self._by_intent.clear()
        self._by_block_key.clear()
        self._states.clear()
        self._finalized_intents.clear()

        if self.dsn:
            await self._load_from_db()
            return
        await self._load_from_redis()

    async def reserve_async(self, request: OrderRequest) -> str:
        async with self._lock:
            key = self.reserve(request)
        try:
            await self._persist_request(request, key, "SUBMITTED")
        except Exception:
            async with self._lock:
                self._forget(request.order_intent_id)
            raise
        return key

    async def try_reserve_async(self, request: OrderRequest) -> str | None:
        async with self._lock:
            if request.order_intent_id in self._finalized_intents:
                return None
            if not self.can_submit(request):
                return None
            key = self.reserve(request)
        try:
            await self._persist_request(request, key, "SUBMITTED")
        except Exception:
            async with self._lock:
                self._forget(request.order_intent_id)
            raise
        return key

    async def mark_unknown_submitted_async(self, request: OrderRequest) -> str:
        async with self._lock:
            key = self.mark_unknown_submitted(request)
        await self._persist_request(request, key, "UNKNOWN_SUBMITTED")
        return key

    async def mark_finalized_async(self, request: OrderRequest, status: str = "FINALIZED") -> None:
        async with self._lock:
            self.mark_finalized(request)
        await self._persist_terminal(request.order_intent_id, status)

    async def _connect(self) -> Any:
        if asyncpg is None:
            raise IdempotencyPersistenceError("asyncpg is not installed")
        return await asyncpg.connect(self.dsn)

    async def _load_from_db(self) -> None:
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                f"""
                SELECT
                    order_intent_id,
                    idempotency_key,
                    account_id,
                    symbol,
                    side,
                    status,
                    created_at
                FROM {self.schema}.order_intents
                """,
            )
        finally:
            await conn.close()

        for row in rows:
            self._remember(
                key=str(row["idempotency_key"]),
                order_intent_id=str(row["order_intent_id"]),
                account_id=str(row["account_id"]),
                symbol=str(row["symbol"]),
                side=str(row["side"]),
                status=str(row["status"]),
                created_at=row["created_at"],
            )

    async def _persist_request(self, request: OrderRequest, key: str, status: str) -> None:
        if self.dsn:
            await self._persist_request_db(request, key, status)
            return
        await self._persist_request_redis(request, key, status)

    async def _persist_request_db(self, request: OrderRequest, key: str, status: str) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                f"""
                INSERT INTO {self.schema}.order_intents (
                    order_intent_id,
                    idempotency_key,
                    account_id,
                    symbol,
                    side,
                    quantity,
                    limit_price,
                    order_type,
                    status,
                    created_at,
                    updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NOW(),NOW())
                ON CONFLICT (order_intent_id)
                DO UPDATE SET
                    idempotency_key = EXCLUDED.idempotency_key,
                    account_id = EXCLUDED.account_id,
                    symbol = EXCLUDED.symbol,
                    side = EXCLUDED.side,
                    quantity = EXCLUDED.quantity,
                    limit_price = EXCLUDED.limit_price,
                    order_type = EXCLUDED.order_type,
                    status = EXCLUDED.status,
                    updated_at = NOW()
                """,
                request.order_intent_id,
                key,
                request.account_id,
                request.symbol,
                str(request.side),
                str(request.quantity),
                str(request.price) if request.price is not None else None,
                request.order_type,
                status,
            )
        finally:
            await conn.close()

    async def _persist_terminal(self, order_intent_id: str, status: str) -> None:
        if self.dsn:
            conn = await self._connect()
            try:
                await conn.execute(
                    f"""
                    UPDATE {self.schema}.order_intents
                    SET status = $2,
                        updated_at = NOW()
                    WHERE order_intent_id = $1
                    """,
                    order_intent_id,
                    status,
                )
            finally:
                await conn.close()
            return
        await self._persist_terminal_redis(order_intent_id, status)

    def _redis_key(self) -> str:
        return f"{self.redis_prefix}:idempotency:{self.schema}"

    async def _redis_client(self) -> Any | None:
        if redis is None or not self.redis_url or self._redis_failed:
            return None
        return redis.from_url(self.redis_url, decode_responses=True)

    async def _load_from_redis(self) -> None:
        client = await self._redis_client()
        if client is None:
            return
        try:
            rows = await client.hgetall(self._redis_key())
        except Exception:
            self._redis_failed = True
            logger.warning("Idempotency Redis fallback is unavailable; using process memory only", exc_info=True)
            return
        finally:
            try:
                await client.aclose()
            except Exception:
                pass

        for raw in rows.values():
            try:
                payload = json.loads(raw)
                status = str(payload.get("status", ""))
                if not status:
                    continue
                created_at = datetime.fromisoformat(str(payload["created_at"]))
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                self._remember(
                    key=str(payload["idempotency_key"]),
                    order_intent_id=str(payload["order_intent_id"]),
                    account_id=str(payload["account_id"]),
                    symbol=str(payload["symbol"]),
                    side=str(payload["side"]),
                    status=status,
                    created_at=created_at,
                )
            except Exception:
                continue

    async def _persist_request_redis(self, request: OrderRequest, key: str, status: str) -> None:
        client = await self._redis_client()
        if client is None:
            return
        payload = {
            "order_intent_id": request.order_intent_id,
            "idempotency_key": key,
            "account_id": request.account_id,
            "symbol": request.symbol,
            "side": str(request.side),
            "quantity": str(request.quantity),
            "limit_price": str(request.price) if request.price is not None else None,
            "order_type": request.order_type,
            "status": status,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            await client.hset(self._redis_key(), key, json.dumps(payload, ensure_ascii=False))
        except Exception:
            self._redis_failed = True
            logger.warning("Idempotency Redis fallback write failed; using process memory only", exc_info=True)
        finally:
            try:
                await client.aclose()
            except Exception:
                pass

    async def _persist_terminal_redis(self, order_intent_id: str, status: str) -> None:
        client = await self._redis_client()
        if client is None:
            return
        key = make_idempotency_key(order_intent_id)
        try:
            raw = await client.hget(self._redis_key(), key)
            if raw is None:
                return
            payload = json.loads(raw)
            payload["status"] = status
            payload["updated_at"] = datetime.now(timezone.utc).isoformat()
            await client.hset(self._redis_key(), key, json.dumps(payload, ensure_ascii=False))
        except Exception:
            self._redis_failed = True
            logger.warning("Idempotency Redis fallback terminal update failed", exc_info=True)
        finally:
            try:
                await client.aclose()
            except Exception:
                pass
