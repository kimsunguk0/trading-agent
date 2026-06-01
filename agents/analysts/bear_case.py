"""Bear-case heuristics for news signals."""

from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from decimal import Decimal

import asyncpg

from core.events.schemas import NewsEvent
from .news_analyst import NewsAnalystOutput
from .catalyst_hunter import CatalystEvent


@dataclass
class BearCaseResult:
    bear_case_flag: bool
    trap_patterns: list[str]
    surged_since_news_pct: Decimal
    trap_risk: Decimal
    payload: dict[str, object]


class BearCaseAnalyzer:
    def __init__(self, schema: str = "trading_paper") -> None:
        self.schema = schema

    async def _surge_since_news(self, symbol: str, happened_at: datetime | None) -> Decimal:
        dsn = os.getenv("DATABASE_URL")
        if not dsn or not happened_at:
            return Decimal("0")
        conn: asyncpg.Connection | None = None
        try:
            conn = await asyncpg.connect(dsn)
            row = await conn.fetchrow(
                f"""
                SELECT
                  FIRST_VALUE(close_price) OVER (ORDER BY bucket_start ASC) AS first_close,
                  LAST_VALUE(close_price) OVER (ORDER BY bucket_start ASC) AS last_close
                FROM {self.schema}.ohlcv
                WHERE symbol = $1
                  AND bucket_start >= $2
                  AND bucket_start >= now() - interval '2 days'
                ORDER BY bucket_start ASC
                LIMIT 1
                """,
                symbol,
                happened_at.replace(tzinfo=timezone.utc),
            )
            if not row:
                return Decimal("0")
            first_close = Decimal(str(row["first_close"] or "0"))
            last_close = Decimal(str(row["last_close"] or "0"))
            if first_close <= Decimal("0"):
                return Decimal("0")
            return (last_close - first_close) / first_close
        except Exception:
            return Decimal("0")
        finally:
            if conn is not None:
                await conn.close()

    async def assess(
        self,
        event: NewsEvent,
        analysis: NewsAnalystOutput,
        catalyst: CatalystEvent,
    ) -> BearCaseResult:
        symbol = str((analysis.symbol_candidates[0].code if analysis.symbol_candidates else ""))
        event_time = event.occurred_at
        surged = await self._surge_since_news(symbol, event_time)
        trap_patterns: list[str] = []
        if surged >= Decimal("0.15"):
            trap_patterns.append("price already surged more than 15% after event")
        if analysis.sentiment == "negative":
            trap_patterns.append("negative sentiment for headline")
        if analysis.time_sensitivity == "long" and not catalyst.breakout_confirmed:
            trap_patterns.append("long horizon and no breakout confirmation")

        bear_flag = surged >= Decimal("0.15") or "negative sentiment for headline" in trap_patterns
        trap_risk = Decimal("1") if bear_flag else Decimal("0")
        return BearCaseResult(
            bear_case_flag=bear_flag,
            trap_patterns=trap_patterns,
            surged_since_news_pct=surged,
            trap_risk=trap_risk,
            payload={
                "symbol": symbol,
                "surged_since_news_pct": str(surged),
                "trap_risk": str(trap_risk),
                "catalyst_score": str(catalyst.catalyst_score),
            },
        )
