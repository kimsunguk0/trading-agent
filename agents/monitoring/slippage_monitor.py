"""Fill-event slippage monitoring for live candidate execution quality."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg
import redis.asyncio as redis

from core.events.bus import RedisStreamBus
from core.events.schemas import EventType, FillEvent, RiskEvent


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


@dataclass
class _CandidateInfo:
    order_intent_id: str
    strategy_id: str
    symbol_code: str
    symbol_market: str
    side: str
    intended_price: Decimal
    quantity: Decimal
    max_slippage_pct: Decimal


class SlippageMonitor:
    """Subscribe to fills, persist slippage, and raise alerts when exceeded."""

    def __init__(
        self,
        stream_prefix: str,
        schema: str,
        redis_url: str = "redis://localhost:6379/0",
        dsn: str | None = None,
    ) -> None:
        self.stream_prefix = stream_prefix
        self.schema = schema
        self.redis_url = redis_url
        self.dsn = dsn
        self.bus = RedisStreamBus(redis_url=redis_url, stream_prefix=stream_prefix)
        self._signal_stream = f"{stream_prefix}.{EventType.SIGNAL.value}"
        self._cumulative_slippage: dict[str, Decimal] = defaultdict(Decimal)
        self._cumulative_count: dict[str, int] = defaultdict(int)

    @property
    def cumulative_slippage(self) -> dict[str, Decimal]:
        return dict(self._cumulative_slippage)

    @property
    def cumulative_fill_count(self) -> dict[str, int]:
        return dict(self._cumulative_count)

    async def _fetch_candidate(self, order_intent_id: str) -> _CandidateInfo | None:
        if not order_intent_id:
            return None

        redis_client = redis.from_url(self.redis_url, decode_responses=True)
        try:
            records = await redis_client.xrevrange(self._signal_stream, count=400)
        finally:
            await redis_client.aclose()

        for _message_id, fields in records:
            raw = fields.get("payload") if isinstance(fields, dict) else None
            if not isinstance(raw, str):
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue
            if payload.get("event_type") != "news_candidate":
                continue
            if payload.get("order_intent_id") != order_intent_id:
                continue

            risk = payload.get("risk") if isinstance(payload.get("risk"), dict) else {}
            execution = payload.get("execution") if isinstance(payload.get("execution"), dict) else {}

            max_slippage = risk.get("max_slippage_pct") if isinstance(risk, dict) else None
            if max_slippage is None:
                max_slippage = execution.get("max_slippage_pct")

            return _CandidateInfo(
                order_intent_id=order_intent_id,
                strategy_id=str(payload.get("strategy_id", "unknown")),
                symbol_code=str(payload.get("code", payload.get("symbol", ""))),
                symbol_market=str(payload.get("market", "KR")),
                side=str(payload.get("side", "BUY")).upper(),
                intended_price=_to_decimal(payload.get("price", payload.get("intended_price", "0"))),
                quantity=_to_decimal(payload.get("quantity", payload.get("order_quantity", "0"))),
                max_slippage_pct=_to_decimal(max_slippage),
            )

        return None

    async def _record_slippage(
        self,
        order_intent_id: str,
        broker_order_id: str,
        strategy_id: str,
        symbol_market: str,
        symbol_code: str,
        intended_price: Decimal,
        filled_price: Decimal,
        slippage_pct: Decimal,
        side: str,
        quantity: Decimal,
        occurred_at: datetime,
    ) -> None:
        if self.dsn is None:
            return

        conn = await asyncpg.connect(self.dsn)
        try:
            await conn.execute(
                f"""
                INSERT INTO {self.schema}.slippage_records (
                    order_intent_id,
                    broker_order_id,
                    symbol_market,
                    symbol_code,
                    strategy_id,
                    intended_price,
                    filled_price,
                    slippage_pct,
                    side,
                    quantity,
                    filled_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                order_intent_id,
                broker_order_id,
                symbol_market,
                symbol_code,
                strategy_id,
                str(intended_price),
                str(filled_price),
                str(slippage_pct),
                side,
                str(quantity),
                occurred_at,
            )
        finally:
            await conn.close()

    async def _emit_slippage_alert(
        self,
        strategy_id: str,
        order_intent_id: str,
        slippage_pct: Decimal,
        max_allowed: Decimal,
    ) -> None:
        payload = {
            "event_type": "slippage_alert",
            "order_intent_id": order_intent_id,
            "strategy_id": strategy_id,
            "slippage_pct": str(slippage_pct),
            "max_slippage_pct": str(max_allowed),
            "occurred_at": datetime.now(timezone.utc).isoformat(),
        }

        await self.bus.publish(
            RiskEvent(
                event_type=EventType.RISK,
                occurred_at=datetime.now(timezone.utc),
                payload={"event_type": "slippage_alert"},
                order_intent_id=order_intent_id,
                stage="slippage",
                passed=False,
                reason=f"slippage_exceeded={slippage_pct}:{max_allowed}",
            )
        )

        if self.bus._client is not None:
            await self.bus._client.xadd(
                f"{self.stream_prefix}.alerts",
                {"payload": json.dumps(payload, ensure_ascii=False)},
            )

    async def _alert_slippage(self, strategy_id: str, order_intent_id: str, slippage_pct: Decimal, max_allowed: Decimal) -> None:
        await self._emit_slippage_alert(
            strategy_id=strategy_id,
            order_intent_id=order_intent_id,
            slippage_pct=slippage_pct,
            max_allowed=max_allowed,
        )

    async def handle_fill(self, event: FillEvent) -> None:
        fill = event.fill
        candidate = await self._fetch_candidate(fill.order_intent_id)
        if candidate is None:
            return
        if candidate.intended_price <= Decimal("0"):
            return

        slippage_pct = ((fill.price - candidate.intended_price) / candidate.intended_price) * Decimal("100")

        self._cumulative_slippage[candidate.strategy_id] += slippage_pct
        self._cumulative_count[candidate.strategy_id] += 1

        await self._record_slippage(
            order_intent_id=fill.order_intent_id,
            broker_order_id=fill.order_id,
            strategy_id=candidate.strategy_id,
            symbol_market=candidate.symbol_market,
            symbol_code=candidate.symbol_code,
            intended_price=candidate.intended_price,
            filled_price=fill.price,
            slippage_pct=slippage_pct,
            side=candidate.side,
            quantity=fill.quantity,
            occurred_at=fill.filled_at,
        )

        if candidate.max_slippage_pct > Decimal("0") and slippage_pct > candidate.max_slippage_pct:
            await self._alert_slippage(
                strategy_id=candidate.strategy_id,
                order_intent_id=fill.order_intent_id,
                slippage_pct=slippage_pct,
                max_allowed=candidate.max_slippage_pct,
            )

    async def run(self) -> None:
        while True:
            async for event in self.bus.subscribe(EventType.FILL):
                if isinstance(event, FillEvent):
                    await self.handle_fill(event)

    async def close(self) -> None:
        await self.bus.close()
