"""Clock protocols, replay bus, and calendar-aware market-open checks."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import AsyncGenerator, Protocol, runtime_checkable

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None

try:
    import exchange_calendars
except Exception:  # pragma: no cover
    exchange_calendars = None

from core.events.schemas import Event, EventType


@runtime_checkable
class Clock(Protocol):
    """Protocol for time providers."""

    def now(self) -> datetime:
        ...


class WallClock:
    """Production wall clock."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


@dataclass
class TickClock:
    """Deterministic replay/test clock."""

    start: datetime
    step_seconds: int = 1

    def __post_init__(self) -> None:
        if self.start.tzinfo is None:
            self.start = self.start.replace(tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self.start

    def tick(self, steps: int = 1) -> datetime:
        if steps < 0:
            raise ValueError("steps must be non-negative")
        self.start = self.start + timedelta(seconds=self.step_seconds * steps)
        return self.start

    def advance_by(self, delta: timedelta) -> datetime:
        if delta.total_seconds() < 0:
            raise ValueError("delta must be non-negative")
        self.start = self.start + delta
        return self.start

    def advance_to(self, target: datetime) -> datetime:
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)
        if target < self.start:
            raise ValueError("cannot move TickClock backwards")
        self.start = target
        return self.start


class ReplayEventBus:
    """In-process event bus for deterministic event replay."""

    def __init__(self, clock: TickClock | None = None, stream_prefix: str = "replay.events") -> None:
        self.clock = clock
        self.stream_prefix = stream_prefix
        self._subscribers: dict[str, list[asyncio.Queue[Event | None]]] = defaultdict(list)
        self._published: list[Event] = []

    @property
    def published_events(self) -> tuple[Event, ...]:
        return tuple(self._published)

    def stream_name(self, event_type: str | EventType) -> str:
        value = event_type.value if isinstance(event_type, EventType) else str(event_type)
        return f"{self.stream_prefix}.{value}"

    def _key(self, event_type: str | EventType) -> str:
        return event_type.value if isinstance(event_type, EventType) else str(event_type)

    async def publish(self, event: Event) -> str:
        if self.clock is not None and event.occurred_at > self.clock.now():
            self.clock.advance_to(event.occurred_at)

        self._published.append(event)
        key = self._key(event.event_type)
        for queue in list(self._subscribers.get(key, [])):
            queue.put_nowait(event)
        return f"replay-{len(self._published)}"

    async def subscribe(self, event_type: str | EventType) -> AsyncGenerator[Event, None]:
        key = self._key(event_type)
        queue: asyncio.Queue[Event | None] = asyncio.Queue()
        self._subscribers[key].append(queue)
        try:
            while True:
                event = await queue.get()
                if event is None:
                    break
                yield event
        finally:
            self._subscribers[key] = [item for item in self._subscribers[key] if item is not queue]

    async def replay(self, events: list[Event] | tuple[Event, ...]) -> None:
        for event in sorted(events, key=lambda item: item.occurred_at):
            if self.clock is not None:
                self.clock.advance_to(event.occurred_at)
            await self.publish(event)

    async def close(self) -> None:
        for queues in list(self._subscribers.values()):
            for queue in list(queues):
                queue.put_nowait(None)


def _fallback_is_open_in_tz(now: datetime, tz_name: str, open_hhmm: tuple[int, int], close_hhmm: tuple[int, int]) -> bool:
    if now.weekday() >= 5:
        return False

    local = now.astimezone(ZoneInfo(tz_name))
    open_at = local.replace(
        hour=open_hhmm[0],
        minute=open_hhmm[1],
        second=0,
        microsecond=0,
    )
    close_at = local.replace(
        hour=close_hhmm[0],
        minute=close_hhmm[1],
        second=0,
        microsecond=0,
    )
    return open_at <= local < close_at


def _calendar_cache() -> dict[str, object]:
    if not exchange_calendars:
        return {}
    if not hasattr(_calendar_cache, "_calendars"):
        setattr(
            _calendar_cache,
            "_calendars",
            {
                "KR": exchange_calendars.get_calendar("XKRX"),
                "US": exchange_calendars.get_calendar("XNYS"),
            },
        )
    return getattr(_calendar_cache, "_calendars")


def _is_open_with_exchange_calendar(now: datetime, market: str) -> bool:
    if pd is None:
        raise RuntimeError("calendar support unavailable")
    calendars = _calendar_cache()
    if market not in calendars:
        return False

    calendar = calendars[market]
    local_tz = "Asia/Seoul" if market == "KR" else "America/New_York"
    local_now = now.astimezone(ZoneInfo(local_tz))
    schedule = calendar.schedule(
        start_date=local_now.date(),
        end_date=local_now.date(),
    )
    if schedule.empty:
        return False
    row = schedule.iloc[0]
    open_at = row["market_open"].tz_convert(ZoneInfo(local_tz))
    close_at = row["market_close"].tz_convert(ZoneInfo(local_tz))
    return open_at <= pd.Timestamp(local_now) < close_at


def is_market_open(market: str = "KR", now: datetime | None = None) -> bool:
    """Return whether the market is open at `now` for the given market.

    market
        KR: KRX (XKRX)
        US: NYSE (XNYS)
    """

    now = now or datetime.now(timezone.utc)
    market = market.upper()
    if market not in {"KR", "US"}:
        return False

    if exchange_calendars is not None:
        try:
            return _is_open_with_exchange_calendar(now, market)
        except Exception:
            pass

    if market == "KR":
        return _fallback_is_open_in_tz(now, "Asia/Seoul", (9, 0), (15, 30))
    return _fallback_is_open_in_tz(now, "America/New_York", (9, 30), (16, 0))
