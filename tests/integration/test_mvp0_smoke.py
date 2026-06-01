from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from decimal import Decimal
from types import SimpleNamespace

import pytest

import core.execution.idempotency as idempotency_module
from agents.decision.decision_engine import DecisionEngine
from agents.monitoring.position_monitor import MonitoredPosition, PositionExitRule, PositionMonitor
from brokers.simulated import SimulatedBrokerAdapter
from core.bootstrap import FatalConfigError, validate_bootstrap
from core.events.schemas import EventType, SignalEvent
from core.execution.engine import ExecutionEngine
from core.execution.idempotency import OrderIdempotencyManager
from core.execution.state_machine import OrderExecutionState, OrderStateMachine
from core.models.market import Side, Symbol
from core.models.order import OrderRequest, RiskCheckResult
from core.risk.compliance import ComplianceAgent
from core.risk.gate import RiskGate
from core.risk.limits import RiskManager
from core.risk.sanity_check import SanityCheckAgent
from core.system_state import SystemState, SystemStateMachine


class _SmokeBus:
    def __init__(self) -> None:
        self.events = []
        self._client = None
        self._fallback_streams = defaultdict(deque)
        self._fallback_subscribers = defaultdict(list)

    def stream_name(self, event_type: str | EventType) -> str:
        value = event_type.value if isinstance(event_type, EventType) else str(event_type)
        return f"paper.events.{value}"

    async def publish(self, event) -> str:
        self.events.append(event)
        return "smoke"


class _RecordingComponent:
    def __init__(self, inner, stages: list[str]) -> None:
        self.inner = inner
        self.stages = stages

    async def check(self, request: OrderRequest) -> RiskCheckResult:
        result = await self.inner.check(request)
        if result.passed:
            self.stages.append(result.stage)
        return result


class _RecordingRiskManager(RiskManager):
    def __init__(self, stages: list[str]) -> None:
        super().__init__()
        self.stages = stages

    async def check(self, request: OrderRequest, account) -> RiskCheckResult:
        result = await super().check(request, account)
        if result.passed:
            self.stages.append(result.stage)
        return result


class _RecordingCompliance(ComplianceAgent):
    def __init__(self, stages: list[str]) -> None:
        super().__init__(dsn=None)
        self.stages = stages

    async def check(self, request: OrderRequest) -> RiskCheckResult:
        result = await super().check(request)
        if result.passed:
            self.stages.append(result.stage)
        return result


class _CountingBroker(SimulatedBrokerAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.submit_count = 0
        self.timeout_probability = Decimal("0")
        self.delay_probability = Decimal("0")
        self.partial_fill_probability = Decimal("0")

    async def submit_order(self, request: OrderRequest):
        self.submit_count += 1
        return await super().submit_order(request)


class _TimeoutBroker:
    def __init__(self) -> None:
        self.submit_count = 0

    async def submit_order(self, request: OrderRequest):
        self.submit_count += 1
        raise asyncio.TimeoutError("smoke unknown submitted")

    async def get_order_status(self, order_intent_id: str):
        return None

    def get_account(self, account_id: str):
        return SimpleNamespace(account_id=account_id, cash_balance=Decimal("1000000"), currency="KRW")


class _FakeRedis:
    def __init__(self, store: dict[str, dict[str, str]]) -> None:
        self.store = store

    async def hset(self, name: str, key: str, value: str) -> None:
        self.store.setdefault(name, {})[key] = value

    async def hget(self, name: str, key: str) -> str | None:
        return self.store.get(name, {}).get(key)

    async def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.store.get(name, {}))

    async def aclose(self) -> None:
        return None


def _recording_gate(stages: list[str]) -> RiskGate:
    return RiskGate(
        sanity_check=_RecordingComponent(SanityCheckAgent(), stages),
        risk_manager=_RecordingRiskManager(stages),
        compliance=_RecordingCompliance(stages),
    )


def _engine(broker, idempotency: OrderIdempotencyManager, system_state: SystemStateMachine, stages: list[str]) -> ExecutionEngine:
    return ExecutionEngine(
        broker=broker,
        state_machine=OrderStateMachine(),
        idempotency=idempotency,
        risk_gate=_recording_gate(stages),
        system_state=system_state,
        account_lookup=broker.get_account,
        broker_timeout_seconds=0.01,
        unknown_poll_attempts=0,
    )


def _signal() -> SignalEvent:
    return SignalEvent(
        event_type=EventType.SIGNAL,
        strategy_id="mvp0_smoke",
        account_id="default",
        symbol=Symbol("005930"),
        side=Side.BUY,
        signal_score=Decimal("1"),
        payload={"market": "KR", "quantity": "2", "price": "100", "order_type": "MARKET"},
    )


@pytest.mark.asyncio
async def test_mvp0_signal_to_fill_status_and_halt_smoke(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", "paper")
    broker = _CountingBroker()
    bus = _SmokeBus()
    system_state = SystemStateMachine()
    stages: list[str] = []
    engine = _engine(broker, OrderIdempotencyManager(), system_state, stages)

    decision = DecisionEngine(broker=broker, bus=bus, risk_gate=RiskGate(), account_id="default")
    order_intent = await decision.process_signal(_signal())
    assert order_intent is not None
    assert order_intent.request.order_intent_id.startswith("OI-")

    result = await engine.submit_order_intent(order_intent.request)
    assert result.state == OrderExecutionState.RECONCILED
    assert result.risk_check.passed is True
    assert stages == ["sanity", "limits", "compliance"]
    assert broker.submit_count == 1
    assert broker._positions[("default", "005930")].quantity == Decimal("2")

    from apps.api.routes import control as control_routes
    from apps.api.routes import orders as order_routes
    from apps.api.routes import status as status_routes

    smoke_state = SimpleNamespace(broker=broker, system_state=system_state, engine=engine)
    monkeypatch.setattr(status_routes, "APP_STATE", smoke_state)
    monkeypatch.setattr(control_routes, "APP_STATE", smoke_state)
    monkeypatch.setattr(order_routes, "APP_STATE", smoke_state)

    status = await status_routes.get_status()
    assert status["account_id"] == "default"
    assert status["cash_balance"] < Decimal("1000000")
    assert status["system_state"] == SystemState.NORMAL.value

    halted = control_routes.halt()
    assert halted == {"state": SystemState.HALTED.value}
    blocked = await order_routes.trigger(order_routes.TriggerRequest(symbol="066570", quantity=Decimal("1")))
    assert blocked["state"] == OrderExecutionState.FAILED.value
    assert blocked["message"] == "system halted"
    assert broker.submit_count == 1


@pytest.mark.asyncio
async def test_unknown_submitted_idempotency_persists_across_manager_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    store: dict[str, dict[str, str]] = {}
    monkeypatch.setattr(idempotency_module.redis, "from_url", lambda *args, **kwargs: _FakeRedis(store))

    broker = _TimeoutBroker()
    system_state = SystemStateMachine()
    stages: list[str] = []
    manager = OrderIdempotencyManager(
        redis_url="redis://fake",
        redis_prefix="paper.events",
        schema="trading_paper",
    )
    await manager.load()
    engine = _engine(broker, manager, system_state, stages)

    request = OrderRequest(
        order_intent_id="OI-20260601-KR-005930-0001",
        account_id="default",
        symbol="005930",
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("100"),
    )
    unknown = await engine.submit_order_intent(request)
    assert unknown.state == OrderExecutionState.UNKNOWN_SUBMITTED
    assert broker.submit_count == 1

    same_intent_retry = await engine.submit_order_intent(request)
    assert same_intent_retry.state == OrderExecutionState.UNKNOWN_SUBMITTED
    assert broker.submit_count == 1

    same_symbol_new_intent = OrderRequest(
        order_intent_id="OI-20260601-KR-005930-0002",
        account_id="default",
        symbol="005930",
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("100"),
    )
    blocked = await engine.submit_order_intent(same_symbol_new_intent)
    assert blocked.message == "blocked by active idempotency duplicate rule"
    assert broker.submit_count == 1

    restarted = OrderIdempotencyManager(
        redis_url="redis://fake",
        redis_prefix="paper.events",
        schema="trading_paper",
    )
    await restarted.load()
    assert not restarted.can_submit(same_symbol_new_intent)
    assert await restarted.try_reserve_async(same_symbol_new_intent) is None
    assert store


@pytest.mark.asyncio
async def test_terminal_order_intent_id_is_not_resubmitted_after_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    store: dict[str, dict[str, str]] = {}
    monkeypatch.setattr(idempotency_module.redis, "from_url", lambda *args, **kwargs: _FakeRedis(store))

    request = OrderRequest(
        order_intent_id="OI-20260601-KR-066570-0001",
        account_id="default",
        symbol="066570",
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("100"),
    )
    manager = OrderIdempotencyManager(redis_url="redis://fake", redis_prefix="paper.events", schema="trading_paper")
    assert await manager.try_reserve_async(request) is not None
    await manager.mark_finalized_async(request, "FILLED")
    assert await manager.try_reserve_async(request) is None

    restarted = OrderIdempotencyManager(redis_url="redis://fake", redis_prefix="paper.events", schema="trading_paper")
    await restarted.load()
    assert await restarted.try_reserve_async(request) is None


@pytest.mark.asyncio
async def test_position_monitor_triggers_take_profit_without_strategy_worker() -> None:
    bus = _SmokeBus()
    monitor = PositionMonitor(
        schema="trading_paper",
        account_id="default",
        bus=bus,  # type: ignore[arg-type]
        rule=PositionExitRule(stop_loss_pct=Decimal("0.03"), take_profit_pct=Decimal("0.05")),
    )
    position = MonitoredPosition(
        account_id="default",
        symbol="005930",
        quantity=Decimal("2"),
        average_price=Decimal("100"),
    )

    async def positions() -> list[MonitoredPosition]:
        return [position]

    async def latest_price(symbol: str) -> Decimal:
        return Decimal("106")

    monitor._fetch_open_positions = positions  # type: ignore[method-assign]
    monitor._fetch_latest_market_price = latest_price  # type: ignore[method-assign]

    decisions = await monitor.run_once()
    assert len(decisions) == 1
    assert decisions[0].reason == "TAKE_PROFIT"
    assert decisions[0].request.side == "SELL"
    assert decisions[0].request.quantity == Decimal("2")
    assert [event.event_type for event in bus.events] == [EventType.ORDER_INTENT, EventType.RISK]


def test_bootstrap_fail_fast_smoke(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "BROKER_KEY_ENV",
        "BROKER_CREDENTIAL_ENV",
        "VAULT_BROKER_ENV",
        "APP_KEY",
        "APP_SECRET",
        "KIWOOM_APP_KEY",
        "KIWOOM_APP_SECRET",
        "KIS_APP_KEY",
        "KIS_APP_SECRET",
        "KIWOOM_LIVE_APP_KEY",
        "KIWOOM_LIVE_APP_SECRET",
        "KIS_LIVE_APP_KEY",
        "KIS_LIVE_APP_SECRET",
    ):
        monkeypatch.delenv(name, raising=False)

    monkeypatch.setenv("ENVIRONMENT", "paper")
    monkeypatch.setenv("OPERATING_MODE", "PAPER")
    monkeypatch.setenv("REDIS_STREAM_PREFIX", "paper.events")
    monkeypatch.setenv("DB_SCHEMA", "trading_paper")
    monkeypatch.setenv("BROKER_ADAPTER", "simulated")
    ok = validate_bootstrap()
    assert ok.environment == "paper"
    assert ok.broker_key_env == "paper"

    monkeypatch.setenv("BROKER_KEY_ENV", "live")
    with pytest.raises(FatalConfigError):
        validate_bootstrap()

    monkeypatch.setenv("BROKER_KEY_ENV", "paper")
    monkeypatch.setenv("DB_SCHEMA", "trading_live")
    with pytest.raises(FatalConfigError):
        validate_bootstrap()
