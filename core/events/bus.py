"""Redis stream bus implementation with in-memory fallback."""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict, deque
from typing import Any, AsyncGenerator, Deque, Dict, List

from pydantic import TypeAdapter

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

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover - environment without redis import
    redis = None


class RedisStreamBus:
    def __init__(self, redis_url: str | None = None, stream_prefix: str | None = None) -> None:
        import os
        redis_url = redis_url or os.getenv("REDIS_URL", "redis://redis:6379/0")
        stream_prefix = stream_prefix or os.getenv("REDIS_STREAM_PREFIX", "paper.events")
        self.redis_url = redis_url
        self.stream_prefix = stream_prefix
        self._client = None
        self._fallback_streams: Dict[str, Deque[str]] = defaultdict(deque)
        self._fallback_subscribers: Dict[str, List[asyncio.Queue[str]]] = defaultdict(list)
        if redis is not None:
            self._client = redis.from_url(redis_url, decode_responses=True)

    def stream_name(self, event_type: str | EventType) -> str:
        if isinstance(event_type, EventType):
            event_type = event_type.value
        return f"{self.stream_prefix}.{event_type}"

    async def publish(self, event: Event) -> str:
        payload = event.model_dump(mode="json", by_alias=True)
        body = json.dumps(payload, ensure_ascii=False)
        stream = self.stream_name(event.event_type)
        if self._client is None:
            self._fallback_streams[stream].append(body)
            for q in list(self._fallback_subscribers[stream]):
                q.put_nowait(body)
            return "fallback"

        message_id = await self._client.xadd(stream, {"payload": body})
        return str(message_id)

    async def subscribe(self, event_type: str | EventType) -> AsyncGenerator[Event, None]:
        stream = self.stream_name(event_type)
        if self._client is None:
            queue: asyncio.Queue[str] = asyncio.Queue()
            self._fallback_subscribers[stream].append(queue)
            try:
                while True:
                    body = await queue.get()
                    yield self._decode(body)
            finally:
                self._fallback_subscribers[stream] = [
                    q for q in self._fallback_subscribers[stream] if q is not queue
                ]
            return

        cursor = "0-0"
        while True:
            messages = await self._client.xread({stream: cursor}, count=10, block=1000)
            if not messages:
                continue
            for _, entries in messages:
                for message_id, payloads in entries:
                    cursor = message_id
                    body = payloads.get("payload")
                    if body is None:
                        continue
                    yield self._decode(body)

    def _decode(self, raw: str) -> Event:
        payload = json.loads(raw)
        event_type = payload.get("event_type")
        if event_type == EventType.MARKET_TICK.value:
            adapter: TypeAdapter[MarketTickEvent] = TypeAdapter(MarketTickEvent)
            return adapter.validate_python(payload)
        if event_type == EventType.SIGNAL.value:
            adapter = TypeAdapter(SignalEvent)
            return adapter.validate_python(payload)
        if event_type == EventType.ORDER_INTENT.value:
            adapter = TypeAdapter(OrderIntentEvent)
            return adapter.validate_python(payload)
        if event_type == EventType.NEWS.value:
            adapter = TypeAdapter(NewsEvent)
            return adapter.validate_python(payload)
        if event_type == EventType.FILL.value:
            adapter = TypeAdapter(FillEvent)
            return adapter.validate_python(payload)
        if event_type == EventType.RISK.value:
            adapter = TypeAdapter(RiskEvent)
            return adapter.validate_python(payload)
        if event_type == EventType.CORPORATE_ACTION.value:
            adapter = TypeAdapter(CorporateActionEvent)
            return adapter.validate_python(payload)

        adapter: TypeAdapter[Event] = TypeAdapter(Event)
        return adapter.validate_python(payload)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
