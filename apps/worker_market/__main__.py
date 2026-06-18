"""Market worker: websocket/poll market ticks -> MarketTickEvent and OHLCV 저장."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import AsyncIterator, Iterable

import asyncpg

import redis.asyncio as redis

from brokers.kiwoom_rest_kr_live import KiwoomRestKrLiveAdapter
from brokers.kiwoom_rest_kr_mock import KiwoomRestKrMockAdapter
from brokers.kis_domestic_kr_live import KISDomesticKrLiveAdapter
from brokers.kis_domestic_kr_mock import KISDomesticKrMockAdapter
from brokers.kis_overseas_live import KISOverseasLiveAdapter
from brokers.kis_overseas_mock import KISOverseasMockAdapter
from brokers.simulated import SimulatedBrokerAdapter
from brokers.toss_invest_future import TossInvestAdapter
from core.clock import is_market_open
from core.events.bus import RedisStreamBus
from core.events.schemas import EventType, MarketTickEvent
from core.models.market import Market, Symbol


@dataclass
class OhlcvBar:
    market: str
    symbol: str
    bucket_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    @classmethod
    def from_price(
        cls,
        market: str,
        symbol: str,
        price: Decimal,
        bucket_start: datetime,
        volume: Decimal,
    ) -> "OhlcvBar":
        return cls(
            market=market,
            symbol=symbol,
            bucket_start=bucket_start,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=volume,
        )

    def update(self, price: Decimal, volume: Decimal) -> None:
        self.close = price
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.volume += volume

    def close_to_close(self) -> bool:
        return self.volume >= Decimal("0")


def _to_decimal(value: object) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _parse_symbol(value: object) -> str:
    if isinstance(value, Symbol):
        return value.value
    if isinstance(value, str):
        return value
    return str(value)


def _floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def _market_from_env() -> str:
    return os.getenv("MARKET", "KR").upper()


def _iter_symbols() -> list[str]:
    market = _market_from_env()
    if market == "US":
        raw = os.getenv("MARKET_SYMBOLS", "AAPL,MSFT,NVDA")
    else:
        raw = os.getenv("MARKET_SYMBOLS", "005930,066570,000660")
    return [s.strip() for s in raw.split(",") if s.strip()]


def _select_broker() -> tuple[object, bool]:
    adapter_name = os.getenv("BROKER_ADAPTER", "simulated").lower()
    market = _market_from_env()

    if adapter_name in {"toss", "toss_invest", "toss_invest_live"}:
        return TossInvestAdapter(), False

    if market == "US":
        if adapter_name in {"kis_overseas_live", "kis_live"}:
            return KISOverseasLiveAdapter(), False
        return KISOverseasMockAdapter(), False

    if adapter_name in {"kiwoom_mock", "kiwoom_live"}:
        return KiwoomRestKrMockAdapter() if adapter_name == "kiwoom_mock" else KiwoomRestKrLiveAdapter(), False

    if adapter_name in {"kis_kr_mock", "kis_domestic_mock", "kis_domestic_kr_mock"}:
        return KISDomesticKrMockAdapter(), False
    if adapter_name in {"kis_kr_live", "kis_domestic_live", "kis_domestic_kr_live"}:
        return KISDomesticKrLiveAdapter(), False

    return SimulatedBrokerAdapter(), True


async def _db_connection():
    dsn = os.getenv("DATABASE_URL", "postgresql://stock:stock@localhost:5432/stock")
    if not dsn:
        return None
    try:
        return await asyncpg.connect(dsn)
    except Exception:
        return None


async def _upsert_ohlcv(conn: asyncpg.Connection, schema: str, bar: OhlcvBar) -> None:
    await conn.execute(
        f"""
        INSERT INTO {schema}.ohlcv (
            bucket_start,
            market,
            symbol,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            created_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8, now())
        ON CONFLICT (bucket_start, market, symbol)
        DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            created_at = now()
        """,
        bar.bucket_start,
        bar.market,
        bar.symbol,
        bar.open,
        bar.high,
        bar.low,
        bar.close,
        bar.volume,
    )


async def _publish_bars(
    bars: dict[tuple[str, str, datetime], OhlcvBar],
    bucket: tuple[str, str, datetime],
    connection: asyncpg.Connection | None,
    schema: str,
    bus: RedisStreamBus,
) -> None:
    bar = bars.pop(bucket)
    if connection is not None:
        try:
            await _upsert_ohlcv(connection, schema, bar)
        except Exception:
            pass

    await bus.publish(
        MarketTickEvent(
            event_type=EventType.MARKET_TICK,
            symbol=Symbol(bar.symbol),
            market=Market(bar.market),
            bid=bar.close,
            ask=bar.close,
            price=bar.close,
            volume=bar.volume,
            occurred_at=bar.bucket_start,
        )
    )


async def _watch_simulated(
    broker: SimulatedBrokerAdapter,
    symbols: Iterable[str],
    market: str,
) -> AsyncIterator[MarketTickEvent]:
    while True:
        for symbol in symbols:
            if not isinstance(symbol, str):
                continue
            price = await broker.get_market_tick(symbol)
            now = datetime.now(timezone.utc)
            yield MarketTickEvent(
                event_type=EventType.MARKET_TICK,
                symbol=Symbol(symbol),
                market=Market(market),
                bid=price,
                ask=price,
                price=price,
                volume=Decimal("0"),
                occurred_at=now,
            )
        await asyncio.sleep(1)


async def _watch_streamed(
    broker: KiwoomRestKrMockAdapter | KiwoomRestKrLiveAdapter | KISDomesticKrMockAdapter | KISDomesticKrLiveAdapter,
    symbols: list[str],
    market: str,
) -> AsyncIterator[MarketTickEvent]:
    async for message in broker.stream_market_ticks(symbols):
        price = _to_decimal(message.get("price"))
        volume = _to_decimal(message.get("volume"))
        symbol = _parse_symbol(message.get("symbol"))
        market_value = str(message.get("market", market))
        occurred_at = message.get("occurred_at") or datetime.now(timezone.utc)
        if isinstance(occurred_at, str):
            occurred_at = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
        yield MarketTickEvent(
            event_type=EventType.MARKET_TICK,
            symbol=Symbol(symbol),
            market=Market(market_value),
            bid=price,
            ask=price,
            price=price,
            volume=volume,
            occurred_at=occurred_at,
        )


async def _watch_polling(
    broker: KISOverseasMockAdapter | KISOverseasLiveAdapter | TossInvestAdapter,
    symbols: list[str],
    market: str,
) -> AsyncIterator[MarketTickEvent]:
    while True:
        for symbol in symbols:
            payload = await broker.get_market_tick(symbol)
            price = _to_decimal(payload.get("price"))
            volume = _to_decimal(payload.get("volume"))
            occurred_at = payload.get("occurred_at") or datetime.now(timezone.utc)
            if isinstance(occurred_at, str):
                occurred_at = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
            yield MarketTickEvent(
                event_type=EventType.MARKET_TICK,
                symbol=Symbol(symbol),
                market=Market(market),
                bid=price,
                ask=price,
                price=price,
                volume=volume,
                occurred_at=occurred_at,
            )
        await asyncio.sleep(1)


async def _refresh_fx_loop(
    market: str,
    broker: object,
    environment: str,
    redis_url: str,
) -> None:
    if market != "US":
        return

    try:
        redis_client = redis.from_url(redis_url, decode_responses=True)
    except Exception:
        return

    last_refreshed = datetime.min.replace(tzinfo=timezone.utc)
    try:
        while True:
            now = datetime.now(timezone.utc)
            if now - last_refreshed < timedelta(minutes=30):
                await asyncio.sleep(30)
                continue

            fx_value = None
            if hasattr(broker, "get_fx_rate"):
                try:
                    fx_value = await getattr(broker, "get_fx_rate")()
                    fx_decimal = _to_decimal(fx_value)
                    key = f"{environment}:fx:USD_KRW"
                    await redis_client.set(key, str(fx_decimal))
                    last_refreshed = now
                except Exception:
                    pass

            if fx_value is None:
                await asyncio.sleep(30)
                continue
    finally:
        await redis_client.aclose()


async def main() -> None:
    market = _market_from_env()
    broker, use_sim = _select_broker()
    symbols = _iter_symbols()
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    stream_prefix = os.getenv("REDIS_STREAM_PREFIX", "paper.events")
    environment = os.getenv("ENVIRONMENT", "paper")
    schema = f"trading_{environment}"

    bus = RedisStreamBus(redis_url=redis_url, stream_prefix=stream_prefix)
    conn = await _db_connection()
    bars: dict[tuple[str, str, datetime], OhlcvBar] = {}

    fx_task: asyncio.Task[None] | None = None
    fx_task = asyncio.create_task(_refresh_fx_loop(market, broker, environment, redis_url))

    try:
        if isinstance(broker, (KiwoomRestKrMockAdapter, KiwoomRestKrLiveAdapter, KISDomesticKrMockAdapter, KISDomesticKrLiveAdapter)):
            tick_iter = _watch_streamed(broker, symbols, market)
        elif isinstance(broker, (KISOverseasMockAdapter, KISOverseasLiveAdapter, TossInvestAdapter)):
            tick_iter = _watch_polling(broker, symbols, market)
        else:
            tick_iter = _watch_simulated(broker, symbols, market)

        async for tick in tick_iter:
            if not is_market_open(market=market, now=tick.occurred_at):
                await asyncio.sleep(5)
                continue

            bucket_start = _floor_minute(tick.occurred_at)
            key = (tick.market.value, tick.symbol.value, bucket_start)

            bar = bars.get(key)
            if bar is None:
                # flush previous bars with same symbol+market
                for existing in list(bars):
                    if existing[0] == tick.market.value and existing[1] == tick.symbol.value and existing[2] < bucket_start:
                        await _publish_bars(bars, existing, conn, schema, bus)
                bars[key] = OhlcvBar.from_price(
                    tick.market.value,
                    tick.symbol.value,
                    _to_decimal(tick.price),
                    bucket_start,
                    _to_decimal(tick.volume),
                )
            else:
                bars[key].update(_to_decimal(tick.price), _to_decimal(tick.volume))

            await bus.publish(tick)
    finally:
        # flush open bars every 10 seconds by scanning old bars
        for key in list(bars):
            if conn is not None:
                await _publish_bars(bars, key, conn, schema, bus)
        if conn is not None:
            await conn.close()
        if fx_task is not None and not fx_task.done():
            fx_task.cancel()
            try:
                await fx_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    asyncio.run(main())
