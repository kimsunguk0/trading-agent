"""Shared state for API process."""

from __future__ import annotations

from core.execution.engine import ExecutionEngine
from core.execution.idempotency import OrderIdempotencyManager
from core.execution.state_machine import OrderStateMachine
from core.risk.gate import RiskGate
from core.system_state import SystemStateMachine
from brokers.simulated import SimulatedBrokerAdapter


class APIState:
    def __init__(self) -> None:
        self.system_state = SystemStateMachine()
        self.state_machine = OrderStateMachine()
        self.idempotency = OrderIdempotencyManager()
        self.risk_gate = RiskGate()
        self.broker = SimulatedBrokerAdapter()

        self.engine = ExecutionEngine(
            broker=self.broker,
            state_machine=self.state_machine,
            idempotency=self.idempotency,
            risk_gate=self.risk_gate,
            system_state=self.system_state,
            account_lookup=self.broker.get_account,
        )


APP_STATE = APIState()
