"""Aggregate weekly performance attribution and push to Telegram."""

from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import asyncpg
from telegram import Bot


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _sqrt(value: Decimal) -> Decimal:
    if value <= Decimal("0"):
        return Decimal("0")
    return value.sqrt()


def _schema() -> str:
    return f"trading_{os.getenv('ENVIRONMENT', 'paper')}"


def _allowed_users() -> list[int]:
    raw = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "")
    return [int(item.strip()) for item in raw.split(",") if item.strip().isdigit()]


@dataclass(frozen=True)
class _Trade:
    strategy_id: str
    regime: str
    realized_pnl: Decimal
    trade_count: int
    pnl_pct: Decimal


class PerformanceAttribution:
    def __init__(self, *, dsn: str | None = None) -> None:
        self.dsn = dsn or os.getenv("DATABASE_URL")
        self.schema = _schema()

    async def _fetch_closed_trades(self) -> list[_Trade]:
        if not self.dsn:
            return []

        conn = await asyncpg.connect(self.dsn)
        try:
            queries = (
                f"""
                SELECT
                    strategy_id,
                    COALESCE(regime, 'UNKNOWN') AS regime,
                    COALESCE(realized_pnl, 0) AS realized_pnl,
                    COALESCE(pnl_pct, 0) AS pnl_pct
                FROM {self.schema}.closed_trades
                WHERE exit_at >= NOW() - INTERVAL '7 days'
                """,
                f"""
                SELECT
                    COALESCE(strategy_id, 'UNKNOWN') AS strategy_id,
                    'UNKNOWN' AS regime,
                    (-slippage_pct) AS realized_pnl,
                    (-slippage_pct) AS pnl_pct
                FROM {self.schema}.slippage_records
                WHERE filled_at >= NOW() - INTERVAL '7 days'
                  AND strategy_id IS NOT NULL
                """,
            )

            rows = []
            for query in queries:
                try:
                    rows = await conn.fetch(query)
                    if rows:
                        break
                except Exception:
                    rows = []
                    continue

            output: list[_Trade] = []
            for row in rows:
                output.append(
                    _Trade(
                        strategy_id=str(row["strategy_id"] or "UNKNOWN"),
                        regime=str(row["regime"] or "UNKNOWN"),
                        realized_pnl=_to_decimal(row["realized_pnl"]),
                        trade_count=1,
                        pnl_pct=_to_decimal(row["pnl_pct"]),
                    )
                )
            return output
        finally:
            await conn.close()

    def _group(self, trades: list[_Trade]) -> dict[tuple[str, str], list[_Trade]]:
        grouped: dict[tuple[str, str], list[_Trade]] = defaultdict(list)
        for trade in trades:
            grouped[(trade.strategy_id, trade.regime)].append(trade)
        return grouped

    def _sharpe(self, values: list[Decimal]) -> Decimal:
        if not values:
            return Decimal("0")
        mean = sum(values, Decimal("0")) / _to_decimal(len(values))
        deltas = [(item - mean) ** 2 for item in values]
        variance = sum(deltas, Decimal("0")) / _to_decimal(len(values))
        stdev = _sqrt(variance)
        if stdev <= Decimal("0"):
            return Decimal("0")
        return (mean / stdev) * Decimal("252").sqrt()

    async def _insert_group(
        self,
        conn: asyncpg.Connection,
        strategy_id: str,
        regime: str,
        period_start: datetime,
        period_end: datetime,
        realized_pnl: Decimal,
        trade_count: int,
        win_rate: Decimal,
        sharpe: Decimal,
        attribution: dict[str, Any],
    ) -> None:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.performance_attribution (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                period_start DATE,
                period_end DATE,
                strategy_id TEXT,
                regime TEXT,
                realized_pnl NUMERIC(20,8),
                trade_count INT,
                win_rate NUMERIC(10,6),
                sharpe NUMERIC(10,6),
                attribution_json JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )

        await conn.execute(
            f"""
            INSERT INTO {self.schema}.performance_attribution (
                period_start,
                period_end,
                strategy_id,
                regime,
                realized_pnl,
                trade_count,
                win_rate,
                sharpe,
                attribution_json
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            period_start.date(),
            period_end.date(),
            strategy_id,
            regime,
            str(realized_pnl),
            trade_count,
            str(win_rate),
            str(sharpe),
            attribution,
        )

    async def weekly_attribution(self) -> None:
        if not self.dsn:
            return

        conn = await asyncpg.connect(self.dsn)
        try:
            trades = await self._fetch_closed_trades()
            grouped = self._group(trades)

            for (strategy_id, regime), bucket in grouped.items():
                pnl_values = [item.realized_pnl for item in bucket]
                realized = sum(pnl_values, Decimal("0"))
                trade_count = sum(item.trade_count for item in bucket)
                win_rate = _to_decimal(sum(Decimal("1") for item in bucket if item.realized_pnl > Decimal("0")))
                if trade_count > 0:
                    win_rate = win_rate / _to_decimal(trade_count)

                sharpe = self._sharpe(pnl_values)
                period_end = datetime.now(timezone.utc)
                period_start = period_end - timedelta(days=7)
                attribution = {
                    "trade_count": trade_count,
                    "max_pnl": str(max(pnl_values)) if pnl_values else "0",
                    "min_pnl": str(min(pnl_values)) if pnl_values else "0",
                }

                await self._insert_group(
                    conn=conn,
                    strategy_id=strategy_id,
                    regime=regime,
                    period_start=period_start,
                    period_end=period_end,
                    realized_pnl=realized,
                    trade_count=trade_count,
                    win_rate=win_rate,
                    sharpe=sharpe,
                    attribution=attribution,
                )

                summary = (
                    f"[{strategy_id}][{regime}] realized_pnl={realized} "
                    f"win_rate={win_rate:.4f} sharpe={sharpe:.4f} trades={trade_count}"
                )
                users = _allowed_users()
                if users:
                    bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN", ""))
                    for user in users:
                        await bot.send_message(chat_id=user, text=summary)
        finally:
            await conn.close()
