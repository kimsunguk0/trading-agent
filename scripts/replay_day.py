"""Replay one trading day of typed events through an in-process bus."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Iterable

from pydantic import TypeAdapter

from core.clock import ReplayEventBus, TickClock
from core.events.schemas import (
    CorporateActionEvent,
    Event,
    EventType,
    FillEvent,
    MarketTickEvent,
    NewsEvent,
    OrderIntentEvent,
    RiskEvent,
    SignalEvent,
)


_EVENT_ADAPTERS: dict[str, TypeAdapter] = {
    EventType.CORPORATE_ACTION.value: TypeAdapter(CorporateActionEvent),
    EventType.FILL.value: TypeAdapter(FillEvent),
    EventType.MARKET_TICK.value: TypeAdapter(MarketTickEvent),
    EventType.NEWS.value: TypeAdapter(NewsEvent),
    EventType.ORDER_INTENT.value: TypeAdapter(OrderIntentEvent),
    EventType.RISK.value: TypeAdapter(RiskEvent),
    EventType.SIGNAL.value: TypeAdapter(SignalEvent),
}


@dataclass(frozen=True)
class ReplayResult:
    day: date
    events_replayed: int
    last_time: datetime
    bus: ReplayEventBus


def date_start_utc(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=timezone.utc)


def decode_event(payload: dict) -> Event:
    event_type = str(payload.get("event_type", ""))
    adapter = _EVENT_ADAPTERS.get(event_type, TypeAdapter(Event))
    return adapter.validate_python(payload)


def _iter_jsonl(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                yield payload


async def load_events_by_date(day: date, events_jsonl: str | Path | None = None) -> list[Event]:
    if events_jsonl is None:
        return []

    path = Path(events_jsonl)
    if not path.exists():
        raise FileNotFoundError(f"replay event file not found: {path}")

    events: list[Event] = []
    for payload in _iter_jsonl(path):
        event = decode_event(payload)
        if event.occurred_at.astimezone(timezone.utc).date() == day:
            events.append(event)
    return sorted(events, key=lambda item: item.occurred_at)


async def replay_day(
    day: date,
    strategy_ids: list[str] | None = None,
    *,
    events: Iterable[Event] | None = None,
    events_jsonl: str | Path | None = None,
) -> ReplayResult:
    clock = TickClock(start=date_start_utc(day))
    bus = ReplayEventBus(clock=clock)

    replay_events = list(events) if events is not None else await load_events_by_date(day, events_jsonl)
    if strategy_ids:
        selected = set(strategy_ids)
        replay_events = [
            event
            for event in replay_events
            if str(getattr(event, "strategy_id", event.payload.get("strategy_id", ""))) in selected
            or event.event_type in {EventType.MARKET_TICK, EventType.NEWS, EventType.CORPORATE_ACTION}
        ]

    await bus.replay(replay_events)
    return ReplayResult(
        day=day,
        events_replayed=len(replay_events),
        last_time=clock.now(),
        bus=bus,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay one day of JSONL events")
    parser.add_argument("day", help="Trading day in YYYY-MM-DD format")
    parser.add_argument("--events-jsonl", help="Path to JSONL typed events")
    parser.add_argument("--strategy-id", action="append", default=[], help="Strategy id to include")
    return parser.parse_args()


async def _amain() -> None:
    args = _parse_args()
    result = await replay_day(
        date.fromisoformat(args.day),
        strategy_ids=list(args.strategy_id or []),
        events_jsonl=args.events_jsonl,
    )
    print(
        json.dumps(
            {
                "day": result.day.isoformat(),
                "events_replayed": result.events_replayed,
                "last_time": result.last_time.isoformat(),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    asyncio.run(_amain())
