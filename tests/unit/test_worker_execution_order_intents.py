from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from agents.decision.decision_engine import DecisionEngine
from apps.worker_execution.__main__ import (
    IdempotencyMappingStore,
    _decode_order_intent_body,
    _extract_order_request,
    _process_order_intent_event,
)
from core.events.bus import RedisStreamBus
from core.events.schemas import ORDER_INTENTS_STREAM, EventType, OrderIntentEvent
from core.models.order import OrderAck, OrderRequest, OrderStatus


def _live_observed_event() -> dict:
    return {
        "event_id": "evt-live-1",
        "event_type": "order_intent",
        "occurred_at": "2026-06-05T09:00:00+09:00",
        "payload": {},
        "request": {
            "order_intent_id": "OI-20260605-KR-000660-0001",
            "account_id": "default",
            "symbol": "000660",
            "side": "BUY",
            "quantity": "1",
            "price": None,
            "order_type": "limit",
        },
    }


def _legacy_payload_event() -> dict:
    return {
        "event_id": "evt-legacy-1",
        "event_type": "order_intent",
        "payload": {
            "order_intent_id": "OI-20260605-KR-005930-0001",
            "account_id": "default",
            "symbol": "005930",
            "side": "SELL",
            "quantity": "2",
            "price": "71000",
            "order_type": "LIMIT",
        },
    }


def test_extract_order_request_from_live_request_structured_event() -> None:
    request = _extract_order_request(_live_observed_event())

    assert request is not None
    assert request.order_intent_id == "OI-20260605-KR-000660-0001"
    assert request.account_id == "default"
    assert request.symbol == "000660"
    assert request.side == "BUY"
    assert request.quantity == Decimal("1")
    assert request.price is None
    assert request.order_type == "limit"


def test_extract_order_request_from_legacy_payload_structured_event() -> None:
    request = _extract_order_request(_legacy_payload_event())

    assert request is not None
    assert request.order_intent_id == "OI-20260605-KR-005930-0001"
    assert request.symbol == "005930"
    assert request.side == "SELL"
    assert request.quantity == Decimal("2")
    assert request.price == Decimal("71000")
    assert request.order_type == "LIMIT"


def test_extract_order_request_broken_empty_event_skips_without_exception(caplog: pytest.LogCaptureFixture) -> None:
    request = _extract_order_request({"event_type": "order_intent", "payload": {}})

    assert request is None
    assert "Skipping order_intent event without a valid order request" in caplog.text


def test_decode_order_intent_body_falls_back_to_raw_dict_on_schema_validation_error() -> None:
    bus = RedisStreamBus(redis_url="redis://unused.local/0", stream_prefix="paper.events")
    body = json.dumps({"event_type": "order_intent", "payload": {}}, ensure_ascii=False)

    decoded = _decode_order_intent_body(bus, body)

    assert decoded == {"event_type": "order_intent", "payload": {}}


def test_decode_order_intent_body_invalid_json_skips_without_exception(caplog: pytest.LogCaptureFixture) -> None:
    bus = RedisStreamBus(redis_url="redis://unused.local/0", stream_prefix="paper.events")

    decoded = _decode_order_intent_body(bus, "{not-json")

    assert decoded is None
    assert "Skipping undecodable order_intent stream message" in caplog.text


@pytest.mark.asyncio
async def test_process_order_intent_event_skips_runtime_errors(caplog: pytest.LogCaptureFixture) -> None:
    class FailingEngine:
        async def submit_order_intent(self, request: OrderRequest):
            raise RuntimeError(f"forced failure for {request.order_intent_id}")

    mapping_store = IdempotencyMappingStore(supports_client_order_id=False, mapping={})

    processed = await _process_order_intent_event(_live_observed_event(), FailingEngine(), mapping_store)

    assert processed is False
    assert mapping_store.mapping == {}
    assert "Skipping order_intent event after processing error" in caplog.text


@pytest.mark.asyncio
async def test_process_order_intent_event_records_mapping_on_success() -> None:
    class SuccessfulEngine:
        async def submit_order_intent(self, request: OrderRequest):
            ack = OrderAck(
                order_id="BRK-1",
                order_intent_id=request.order_intent_id,
                status=OrderStatus.SUBMITTED,
                filled_quantity=Decimal("0"),
                total_quantity=request.quantity,
            )
            return SimpleNamespace(ack=ack)

    mapping_store = IdempotencyMappingStore(supports_client_order_id=False, mapping={})

    processed = await _process_order_intent_event(_live_observed_event(), SuccessfulEngine(), mapping_store)

    assert processed is True
    assert mapping_store.get("OI-20260605-KR-000660-0001") == "BRK-1"


@pytest.mark.asyncio
async def test_decision_engine_publishes_only_canonical_plural_order_intents_stream() -> None:
    bus = RedisStreamBus(redis_url="redis://unused.local/0", stream_prefix="paper.events")
    bus._client = None
    engine = DecisionEngine(broker=SimpleNamespace(), bus=bus, account_id="default")
    request = OrderRequest(
        order_intent_id="OI-20260605-KR-000660-0001",
        account_id="default",
        symbol="000660",
        side="BUY",
        quantity=Decimal("1"),
        price=None,
        order_type="limit",
    )

    await engine._publish_order_intent(OrderIntentEvent(event_type=EventType.ORDER_INTENT, request=request))

    streams = dict(bus._fallback_streams)
    plural_stream = bus.stream_name(ORDER_INTENTS_STREAM)
    singular_stream = bus.stream_name(EventType.ORDER_INTENT)

    assert plural_stream == "paper.events.order_intents"
    assert plural_stream in streams
    assert singular_stream not in streams
    assert len(streams[plural_stream]) == 1

    body = json.loads(streams[plural_stream][0])
    assert body["event_type"] == "order_intent"
    assert body["payload"] == {}
    assert body["request"]["order_intent_id"] == request.order_intent_id
