"""Market regime classifier from OHLCV data."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncio
import yaml

import asyncpg

from core.events.bus import RedisStreamBus


try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return Decimal("1") if value else Decimal("0")
    return Decimal(str(value))


@dataclass
class RegimeConfig:
    lookback_days: int
    regime_v1: dict[str, dict[str, Decimal]]


def _load_regime_config(path: str = "configs/regime/regime_v1.yaml") -> RegimeConfig:
    payload = yaml.safe_load(open(path, "r", encoding="utf-8").read())
    lookback_days = int(payload.get("regime_v1", {}).get("lookback_days", 20))
    cfg: dict[str, dict[str, Decimal]] = {}
    for regime_name, values in payload.get("regime_v1", {}).items():
        if regime_name == "lookback_days":
            continue
        if not isinstance(values, dict):
            continue
        cfg[regime_name] = {str(k): _to_decimal(v) for k, v in values.items()}
    return RegimeConfig(lookback_days=lookback_days, regime_v1=cfg)


def classify_regime_from_ohlcv(rows: list[dict[str, Any]], cfg: RegimeConfig) -> str:
    if len(rows) < 2:
        return "unknown"

    closes = [_to_decimal(r.get("close_price", "0")) for r in rows]
    highs = [_to_decimal(r.get("high_price", "0")) for r in rows]
    lows = [_to_decimal(r.get("low_price", "0")) for r in rows]
    prices = closes

    ma20 = sum(prices) / Decimal(str(len(prices)))
    last_price = prices[-1]
    if ma20 <= Decimal("0"):
        return "unknown"

    total_tr = Decimal("0")
    for idx in range(1, len(rows)):
        prev_close = prices[idx - 1]
        high = highs[idx]
        low = lows[idx]
        if prev_close <= Decimal("0"):
            prev_close = Decimal("1")
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        total_tr += tr
    atr = total_tr / Decimal(str(len(rows) - 1))
    atr_pct = atr / last_price if last_price > 0 else Decimal("0")
    close_move_total = Decimal("0")
    for idx in range(1, len(prices)):
        close_move_total += abs(prices[idx] - prices[idx - 1])
    close_vol = close_move_total / Decimal(str(len(prices) - 1))
    close_vol_pct = close_vol / last_price if last_price > 0 else Decimal("0")

    dev = abs(last_price - ma20) / ma20

    panic_cfg = cfg.regime_v1.get("panic", {})
    bull_vol_cfg = cfg.regime_v1.get("bull_volatile", {})
    bull_trend_cfg = cfg.regime_v1.get("bull_trend", {})
    range_cfg = cfg.regime_v1.get("range", {})
    bear_cfg = cfg.regime_v1.get("bear_trend", {})

    if atr_pct >= panic_cfg.get("atr_pct_min", Decimal("0.04")):
        return "panic"

    if last_price >= ma20 and bull_vol_cfg.get("atr_pct_min", Decimal("0.025")) <= close_vol_pct < Decimal("0.04"):
        return "bull_volatile"

    if last_price >= ma20 and close_vol_pct <= bull_trend_cfg.get("atr_pct_max", Decimal("0.025")):
        return "bull_trend"

    if dev <= range_cfg.get("ma20_deviation_pct_max", Decimal("0.02")):
        return "range"

    if last_price < ma20 and bear_cfg.get("price_below_ma20", True):
        return "bear_trend"

    # Melt-up heuristic: strong trend + positive intraday momentum.
    if last_price >= ma20 and (prices[-1] - prices[0]) > Decimal("0"):
        return "melt_up"
    return "range"


class MarketRegimeWorker:
    def __init__(
        self,
        environment: str = "paper",
        redis_url: str = "redis://localhost:6379/0",
        symbols: list[str] | None = None,
        market: str = "KR",
        schema: str | None = None,
        run_interval_seconds: int = 300,
    ) -> None:
        self.environment = environment
        self.redis_url = redis_url
        self.symbols = symbols or ["1001", "KS200"]
        self.market = market
        self.schema = schema or f"trading_{environment}"
        self.run_interval_seconds = run_interval_seconds
        self.bus = RedisStreamBus(redis_url=redis_url, stream_prefix=f"{environment}.events")
        self.config = _load_regime_config()
        self._redis = redis.from_url(redis_url, decode_responses=True) if redis is not None else None

    async def _fetch_ohlcv(self, symbol: str) -> list[dict[str, Any]]:
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            return []
        conn = await asyncpg.connect(dsn)
        try:
            rows = await conn.fetch(
                f"""
                SELECT bucket_start, open_price, high_price, low_price, close_price
                FROM {self.schema}.ohlcv
                WHERE market = $1
                  AND symbol = $2
                  AND bucket_start >= now() - interval '{int(self.config.lookback_days)} days'
                ORDER BY bucket_start ASC
                """,
                self.market,
                symbol,
            )
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def _cache_regime(self, regime: str) -> None:
        if redis is None:
            return
        key = f"{self.environment}:regime:current"
        async with redis.from_url(self.redis_url, decode_responses=True) as client:  # type: ignore[union-attr]
            await client.set(
                key,
                f'{{"regime":"{regime}","updated_at":"{datetime.now(timezone.utc).isoformat()}"}}',
            )

    async def run_once(self) -> str:
        for symbol in self.symbols:
            rows = await self._fetch_ohlcv(symbol)
            if rows:
                regime = classify_regime_from_ohlcv(rows, self.config)
                payload = {
                    "regime": regime,
                    "symbol": symbol,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                if self._redis is not None:
                    await self._redis.xadd(self.bus.stream_name("regime"), {"payload": str(payload).replace("'", '"')})
                await self._cache_regime(regime)
                return regime
        return "unknown"

    async def run(self) -> None:
        while True:
            await self.run_once()
            await asyncio.sleep(max(60, int(self.run_interval_seconds)))
