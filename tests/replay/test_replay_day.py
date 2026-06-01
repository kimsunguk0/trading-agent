from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from core.clock import ReplayEventBus, TickClock
from core.events.schemas import EventType, MarketTickEvent
from core.models.market import Market, Symbol
from scripts.replay_day import replay_day


def _tick(symbol: str, occurred_at: datetime) -> MarketTickEvent:
    return MarketTickEvent(
        event_type=EventType.MARKET_TICK,
        symbol=Symbol(symbol),
        market=Market.KR,
        bid=Decimal("100"),
        ask=Decimal("101"),
        price=Decimal("100"),
        volume=Decimal("10"),
        occurred_at=occurred_at,
    )


@pytest.mark.asyncio
async def test_replay_event_bus_delivers_tick_and_advances_clock() -> None:
    start = datetime(2026, 5, 29, tzinfo=timezone.utc)
    clock = TickClock(start=start)
    bus = ReplayEventBus(clock=clock)

    async def receive_one() -> MarketTickEvent:
        async for event in bus.subscribe(EventType.MARKET_TICK):
            return event  # type: ignore[return-value]
        raise AssertionError("subscription closed")

    task = asyncio.create_task(receive_one())
    await asyncio.sleep(0)

    event = _tick("005930", start + timedelta(minutes=1))
    await bus.publish(event)

    received = await asyncio.wait_for(task, timeout=1)
    assert received.symbol == "005930"
    assert clock.now() == event.occurred_at


@pytest.mark.asyncio
async def test_replay_day_sorts_events_in_time_order() -> None:
    day = date(2026, 5, 29)
    start = datetime(2026, 5, 29, tzinfo=timezone.utc)
    late = _tick("005930", start + timedelta(minutes=2))
    early = _tick("066570", start + timedelta(minutes=1))

    result = await replay_day(day, events=[late, early])

    assert result.events_replayed == 2
    assert [event.symbol for event in result.bus.published_events] == ["066570", "005930"]
    assert result.last_time == late.occurred_at
