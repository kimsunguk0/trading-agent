"""Execution worker consuming order intents and running execution engine."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from core.execution.engine import ExecutionEngine
from core.events.bus import RedisStreamBus
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
    else:
        adapter = SimulatedBrokerAdapter()

    if not hasattr(adapter, "capabilities"):
        return adapter, True
    return adapter, bool(getattr(adapter.capabilities, "supports_client_order_id", False))


def _extract_order_request(payload: dict) -> OrderRequest:
    if isinstance(payload.get("request"), dict):
        return OrderRequest(**payload["request"])
    return OrderRequest(**payload)


def _account_lookup(adapter: object, account_id: str):
    if hasattr(adapter, "get_account"):
        return adapter.get_account(account_id)  # type: ignore[no-any-return]
    return None


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

    async for event in bus.subscribe("order_intents"):
        request = _extract_order_request(event.payload)
        result = await engine.submit_order_intent(request)

        if not mapping_store.supports_client_order_id and result.ack is not None:
            mapping_store.upsert(request.order_intent_id, result.ack.order_id)

        # Keep mapping even when supports client id is unavailable in broker schema.
        # In this MVP stage, this is used as an in-memory operational backreference.


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
