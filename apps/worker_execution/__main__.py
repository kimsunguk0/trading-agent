"""Execution worker consuming order intents and running execution engine."""

from __future__ import annotations

import inspect
import json
import logging
import os
from dataclasses import dataclass
from typing import Any, AsyncIterator

from core.execution.engine import ExecutionEngine
from core.events.bus import RedisStreamBus
from core.events.schemas import ORDER_INTENTS_STREAM
from core.execution.idempotency import OrderIdempotencyManager
from core.execution.state_machine import OrderStateMachine
from core.models.order import OrderRequest
from core.risk.gate import RiskGate
from core.system_state import SystemStateMachine
from brokers.simulated import SimulatedBrokerAdapter

try:
    from brokers.kiwoom_rest_kr_mock import KiwoomRestKrMockAdapter
except ImportError:  # pragma: no cover - optional dependency fallback
    KiwoomRestKrMockAdapter = None

try:
    from brokers.kiwoom_rest_kr_live import KiwoomRestKrLiveAdapter
except ImportError:  # pragma: no cover - optional dependency fallback
    KiwoomRestKrLiveAdapter = None

try:
    from brokers.kis_domestic_kr_mock import KISDomesticKrMockAdapter
except ImportError:  # pragma: no cover - optional dependency fallback
    KISDomesticKrMockAdapter = None

try:
    from brokers.kis_domestic_kr_live import KISDomesticKrLiveAdapter
except ImportError:  # pragma: no cover - optional dependency fallback
    KISDomesticKrLiveAdapter = None


logger = logging.getLogger(__name__)


@dataclass
class IdempotencyMappingStore:
    supports_client_order_id: bool
    mapping: dict[str, str]

    def upsert(self, key: str, broker_order_id: str) -> None:
        self.mapping[key] = broker_order_id

    def get(self, key: str) -> str | None:
        return self.mapping.get(key)


def _select_broker() -> tuple[object, bool]:
    adapter_name = os.getenv("BROKER_ADAPTER", "simulated").lower()
    if adapter_name == "kiwoom_mock" and KiwoomRestKrMockAdapter is not None:
        try:
            adapter = KiwoomRestKrMockAdapter()
        except Exception:
            logger.warning("Failed to initialize Kiwoom mock adapter; using simulated broker.", exc_info=True)
            return SimulatedBrokerAdapter(), True
    elif adapter_name == "kiwoom_mock":
        logger.warning("Kiwoom mock adapter import failed; using simulated-only mode.")
        return SimulatedBrokerAdapter(), True
    elif adapter_name == "kiwoom_live" and KiwoomRestKrLiveAdapter is not None:
        try:
            adapter = KiwoomRestKrLiveAdapter()
        except Exception:
            logger.warning("Failed to initialize Kiwoom live adapter; using simulated broker.", exc_info=True)
            return SimulatedBrokerAdapter(), True
    elif adapter_name == "kiwoom_live":
        logger.warning("Kiwoom live adapter import failed; using simulated-only mode.")
        return SimulatedBrokerAdapter(), True
    elif adapter_name in {"kis_kr_mock", "kis_domestic_mock", "kis_domestic_kr_mock"} and KISDomesticKrMockAdapter is not None:
        try:
            adapter = KISDomesticKrMockAdapter()
        except Exception:
            logger.warning("Failed to initialize KIS KR mock adapter; using simulated broker.", exc_info=True)
            return SimulatedBrokerAdapter(), True
    elif adapter_name in {"kis_kr_live", "kis_domestic_live", "kis_domestic_kr_live"} and KISDomesticKrLiveAdapter is not None:
        try:
            adapter = KISDomesticKrLiveAdapter()
        except Exception:
            logger.warning("Failed to initialize KIS KR live adapter; using simulated broker.", exc_info=True)
            return SimulatedBrokerAdapter(), True
    else:
        adapter = SimulatedBrokerAdapter()

    if not hasattr(adapter, "capabilities"):
        return adapter, True
    return adapter, bool(getattr(adapter.capabilities, "supports_client_order_id", False))


def _safe_for_log(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    return value


def _try_order_request(candidate: Any) -> OrderRequest | None:
    if isinstance(candidate, OrderRequest):
        return candidate
    if not isinstance(candidate, dict) or not candidate:
        return None
    try:
        return OrderRequest(**candidate)
    except Exception:
        return None


def _extract_order_request(event_or_payload: Any) -> OrderRequest | None:
    """Extract OrderRequest from current request-shaped or legacy payload events."""

    candidates: list[Any] = []
    if isinstance(event_or_payload, dict):
        candidates.append(event_or_payload.get("request"))
        payload = event_or_payload.get("payload")
        if isinstance(payload, dict):
            candidates.append(payload.get("request"))
            candidates.append(payload)
        candidates.append(event_or_payload)
    else:
        candidates.append(getattr(event_or_payload, "request", None))
        payload = getattr(event_or_payload, "payload", None)
        if isinstance(payload, dict):
            candidates.append(payload.get("request"))
            candidates.append(payload)

    for candidate in candidates:
        request = _try_order_request(candidate)
        if request is not None:
            return request

    logger.warning(
        "Skipping order_intent event without a valid order request.",
        extra={"order_intent_event": _safe_for_log(event_or_payload)},
    )
    return None


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _account_lookup(adapter: object, account_id: str):
    for method_name in ("get_account", "get_cash", "get_cash_snapshot"):
        accessor = getattr(adapter, method_name, None)
        if not callable(accessor):
            continue
        account = await _maybe_await(accessor(account_id))
        if account is not None:
            return account
    return None


async def _process_order_intent_event(event: Any, engine: ExecutionEngine, mapping_store: IdempotencyMappingStore) -> bool:
    try:
        request = _extract_order_request(event)
        if request is None:
            return False

        result = await engine.submit_order_intent(request)

        if not mapping_store.supports_client_order_id and result.ack is not None:
            mapping_store.upsert(request.order_intent_id, result.ack.order_id)
        return True
    except Exception:
        logger.exception(
            "Skipping order_intent event after processing error.",
            extra={"order_intent_event": _safe_for_log(event)},
        )
        return False


def _decode_order_intent_body(bus: RedisStreamBus, body: str | None) -> Any | None:
    if body is None:
        return None
    try:
        return bus._decode(body)
    except Exception:
        try:
            payload = json.loads(body)
        except Exception:
            logger.warning(
                "Skipping undecodable order_intent stream message.",
                extra={"order_intent_stream_body": body},
                exc_info=True,
            )
            return None

        if isinstance(payload, dict):
            return payload

        logger.warning(
            "Skipping non-object order_intent stream message.",
            extra={"order_intent_stream_body": body},
        )
        return None


async def _subscribe_order_intent_events(bus: RedisStreamBus) -> AsyncIterator[Any]:
    """Read canonical order-intent stream while isolating bad messages."""

    stream = bus.stream_name(ORDER_INTENTS_STREAM)
    if bus._client is None:
        import asyncio

        queue: asyncio.Queue[str] = asyncio.Queue()
        bus._fallback_subscribers[stream].append(queue)
        try:
            while True:
                body = await queue.get()
                event = _decode_order_intent_body(bus, body)
                if event is not None:
                    yield event
        finally:
            bus._fallback_subscribers[stream] = [
                q for q in bus._fallback_subscribers[stream] if q is not queue
            ]
        return

    cursor = "0-0"
    while True:
        messages = await bus._client.xread({stream: cursor}, count=10, block=1000)
        if not messages:
            continue
        for _, entries in messages:
            for message_id, payloads in entries:
                cursor = message_id
                event = _decode_order_intent_body(bus, payloads.get("payload"))
                if event is not None:
                    yield event


async def main() -> None:
    bus = RedisStreamBus()
    broker, supports_client_order_id = _select_broker()
    account_lookup = lambda account_id: _account_lookup(broker, account_id)
    environment = os.getenv("ENVIRONMENT", "paper").lower()
    schema = os.getenv("DB_SCHEMA", f"trading_{environment}")
    idempotency = OrderIdempotencyManager(
        dsn=os.getenv("DATABASE_URL"),
        schema=schema,
        redis_url=os.getenv("REDIS_URL"),
        redis_prefix=os.getenv("REDIS_STREAM_PREFIX", f"{environment}.events"),
    )
    await idempotency.load()

    engine = ExecutionEngine(
        broker=broker,
        state_machine=OrderStateMachine(),
        idempotency=idempotency,
        risk_gate=RiskGate(),
        system_state=SystemStateMachine(),
        account_lookup=account_lookup,
    )

    mapping_store = IdempotencyMappingStore(
        supports_client_order_id=supports_client_order_id,
        mapping={},
    )

    # Canonical executable order-intent stream:
    # {REDIS_STREAM_PREFIX}.order_intents. The payload event_type remains
    # singular "order_intent" for schema compatibility.
    async for event in _subscribe_order_intent_events(bus):
        await _process_order_intent_event(event, engine, mapping_store)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
