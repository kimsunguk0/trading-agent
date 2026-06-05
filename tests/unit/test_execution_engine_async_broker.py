from __future__ import annotations

from decimal import Decimal

import pytest

from apps.worker_execution.__main__ import _account_lookup
from brokers.simulated import SimulatedBrokerAdapter
from core.execution.engine import ExecutionEngine
from core.execution.idempotency import OrderIdempotencyManager
from core.execution.state_machine import OrderExecutionState, OrderStateMachine
from core.models.order import OrderAck, OrderRequest, OrderStatus
from core.models.portfolio import CashSnapshot
from core.risk.gate import RiskGate
from core.system_state import SystemStateMachine


class _AsyncCashPlaceOrderBroker:
    def __init__(self) -> None:
        self.cash_calls: list[str] = []
        self.place_order_calls: list[OrderRequest] = []
        self.status_calls: list[str] = []

    async def get_cash(self, account_id: str) -> CashSnapshot:
        self.cash_calls.append(account_id)
        return CashSnapshot(
            account_id=account_id,
            cash_balance=Decimal("100000"),
            available_cash=Decimal("90000"),
            reserved_cash=Decimal("0"),
        )

    async def place_order(self, request: OrderRequest) -> OrderAck:
        self.place_order_calls.append(request)
        return OrderAck(
            order_id="0081878",
            order_intent_id=request.order_intent_id,
            status=OrderStatus.FILLED,
            filled_quantity=request.quantity,
            total_quantity=request.quantity,
            average_fill_price=request.price,
        )

    async def get_order_status(self, order_intent_id: str) -> OrderAck | None:
        self.status_calls.append(order_intent_id)
        return None


class _FailingAsyncBroker(_AsyncCashPlaceOrderBroker):
    async def place_order(self, request: OrderRequest) -> OrderAck:
        self.place_order_calls.append(request)
        raise RuntimeError("kiwoom api failure")


def _request(order_intent_id: str = "OI-ASYNC-1") -> OrderRequest:
    return OrderRequest(
        order_intent_id=order_intent_id,
        account_id="default",
        symbol="000660",
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("100"),
        order_type="LIMIT",
    )


def _engine(broker, account_lookup, *, timeout: float = 0.2) -> ExecutionEngine:
    return ExecutionEngine(
        broker=broker,
        state_machine=OrderStateMachine(),
        idempotency=OrderIdempotencyManager(),
        risk_gate=RiskGate(),
        system_state=SystemStateMachine(),
        account_lookup=account_lookup,
        broker_timeout_seconds=timeout,
        unknown_poll_attempts=0,
    )


@pytest.mark.asyncio
async def test_submit_order_intent_awaits_async_get_cash_and_async_place_order() -> None:
    broker = _AsyncCashPlaceOrderBroker()
    engine = _engine(broker, broker.get_cash)

    result = await engine.submit_order_intent(_request())

    assert result.risk_check.passed is True
    assert result.state == OrderExecutionState.RECONCILED
    assert result.ack is not None
    assert result.ack.order_id == "0081878"
    assert broker.cash_calls == ["default"]
    assert [request.order_intent_id for request in broker.place_order_calls] == ["OI-ASYNC-1"]


@pytest.mark.asyncio
async def test_worker_account_lookup_uses_async_get_cash_when_get_account_is_absent() -> None:
    broker = _AsyncCashPlaceOrderBroker()

    account = await _account_lookup(broker, "default")

    assert isinstance(account, CashSnapshot)
    assert account.cash_balance == Decimal("100000")
    assert broker.cash_calls == ["default"]


@pytest.mark.asyncio
async def test_submit_order_intent_keeps_sync_simulated_get_account_path_working() -> None:
    broker = SimulatedBrokerAdapter(initial_cash=Decimal("100000"))
    broker.delay_probability = Decimal("0")
    broker.timeout_probability = Decimal("0")
    broker.partial_fill_probability = Decimal("0")
    engine = _engine(broker, broker.get_account)

    result = await engine.submit_order_intent(_request("OI-SYNC-1"))

    assert result.risk_check.passed is True
    assert result.state == OrderExecutionState.RECONCILED
    assert result.ack is not None
    assert result.ack.order_id == "ORD-OI-SYNC-1"


@pytest.mark.asyncio
async def test_submit_order_intent_returns_failed_result_on_broker_exception() -> None:
    broker = _FailingAsyncBroker()
    engine = _engine(broker, broker.get_cash)

    result = await engine.submit_order_intent(_request("OI-FAIL-1"))

    assert result.state == OrderExecutionState.FAILED
    assert result.ack is None
    assert result.risk_check.passed is True
    assert result.message == "broker_error:RuntimeError"
    assert [request.order_intent_id for request in broker.place_order_calls] == ["OI-FAIL-1"]
