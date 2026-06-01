"""Verification checks for news signals."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from dataclasses import dataclass

import asyncpg


from core.events.schemas import NewsEvent


@dataclass
class VerificationResult:
    duplicate_flag: bool
    multi_source_confirmed: bool
    dart_original_present: bool
    verification_passed: bool
    payload: dict[str, object]


class VerificationAgent:
    def __init__(self, schema: str = "trading_paper") -> None:
        self.schema = schema

    async def _exists_news_dupe(self, body_hash: str) -> bool:
        if not body_hash:
            return False
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            return False
        conn: asyncpg.Connection | None = None
        try:
            conn = await asyncpg.connect(dsn)
            cnt = await conn.fetchval(
                f"""
                SELECT COUNT(*)
                FROM {self.schema}.news_items
                WHERE body_hash = $1
                  AND collected_at >= now() - interval '48 hours'
                """,
                body_hash,
            )
            return int(cnt or 0) > 0
        except Exception:
            return False
        finally:
            if conn is not None:
                await conn.close()

    async def _has_multi_source(self, body_hash: str, source: str) -> bool:
        if not body_hash:
            return False
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            return False
        conn: asyncpg.Connection | None = None
        try:
            conn = await asyncpg.connect(dsn)
            cnt = await conn.fetchval(
                f"""
                SELECT COUNT(DISTINCT source)
                FROM {self.schema}.news_items
                WHERE body_hash = $1 AND source <> $2
                """,
                body_hash,
                source,
            )
            return int(cnt or 0) >= 1
        except Exception:
            return False
        finally:
            if conn is not None:
                await conn.close()

    async def _has_dart_original(self, symbol: str | None) -> bool:
        if not symbol:
            return False
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            return False
        conn: asyncpg.Connection | None = None
        try:
            conn = await asyncpg.connect(dsn)
            cnt = await conn.fetchval(
                f"""
                SELECT COUNT(*)
                FROM {self.schema}.corporate_actions
                WHERE symbol = $1
                  AND action_type IN ('공시', 'DART', 'DIVIDEND', 'BONUS')
                  AND as_of >= now() - interval '30 days'
                """,
                symbol,
            )
            return int(cnt or 0) >= 1
        except Exception:
            return False
        finally:
            if conn is not None:
                await conn.close()

    async def assess(self, event: NewsEvent, *, symbol: str | None = None) -> VerificationResult:
        body_hash = str(event.payload.get("body_hash", "")) if isinstance(event.payload, dict) else ""
        source = str(event.source)
        duplicate = await self._exists_news_dupe(body_hash)
        multi_source = await self._has_multi_source(body_hash, source)
        dart_found = await self._has_dart_original(symbol)
        passed = not duplicate and (not symbol or not self._symbol_stopped(symbol))
        return VerificationResult(
            duplicate_flag=duplicate,
            multi_source_confirmed=multi_source,
            dart_original_present=dart_found,
            verification_passed=passed,
            payload={
                "body_hash": body_hash,
                "duplicate_flag": duplicate,
                "multi_source": multi_source,
                "dart_original_present": dart_found,
                "verified_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    def _symbol_stopped(self, symbol: str) -> bool:
        return False
