"""Liquidity filter helpers for strategy symbols."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg


@dataclass
class LiquiditySnapshot:
    updated_at: datetime
    values: dict[str, Decimal]


async def load_liquidity_snapshot(
    *,
    symbols: list[str],
    schema: str,
    market: str,
    min_avg_value_20d_krw: Decimal,
    dsn: str | None = None,
) -> LiquiditySnapshot:
    symbols = [s for s in symbols if s]
    if not symbols:
        return LiquiditySnapshot(datetime.utcnow(), {})

    dsn = dsn or os.getenv("DATABASE_URL")
    if not dsn:
        return LiquiditySnapshot(datetime.utcnow(), {})

    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            f"""
            SELECT symbol, AVG(close_price * volume) AS avg_value_20d
            FROM {schema}.ohlcv
            WHERE market = $1
              AND symbol = ANY($2)
              AND bucket_start >= now() - interval '20 days'
            GROUP BY symbol
            """,
            market,
            symbols,
        )
    finally:
        await conn.close()

    values: dict[str, Decimal] = {}
    for row in rows:
        value = row["avg_value_20d"]
        if value is None:
            continue
        threshold = Decimal(str(value))
        if threshold >= min_avg_value_20d_krw:
            values[str(row["symbol"])] = threshold
    return LiquiditySnapshot(datetime.utcnow(), values)
