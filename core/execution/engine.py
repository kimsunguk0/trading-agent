"""Execution engine skeleton with UNKNOWN_SUBMITTED handling."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable

from core.events.schemas import RiskEvent
from core.execution.idempotency import OrderIdempotencyManager
from core.execution.state_machine import (
    OrderExecutionEvent,
    OrderExecutionState,
    OrderStateMachine,
)
from core.models.order import OrderAck, OrderRequest, OrderStatus, RiskCheckResult
from core.risk.gate import RiskGate
from core.system_state import SystemStateMachine
from core.models.portfolio import Account
from brokers.base import BrokerAdapter


@dataclass
class ExecutionResult:
    order_intent_id: str
    state: OrderExecutionState
    risk_check: RiskCheckResult
    ack: OrderAck | None
    message: str


class ExecutionEngine:
    def __init__(
        self,
        broker: BrokerAdapter,
        state_machine: OrderStateMachine,
        idempotency: OrderIdempotencyManager,
        risk_gate: RiskGate,
        system_state: SystemStateMachine,
        account_lookup: Callable[[str], Account | None],
        broker_timeout_seconds: float = 1.5,
        unknown_poll_attempts: int = 3,
    ) -> None:
        self.broker = broker
        self.state_machine = state_machine
        self.idempotency = idempotency
        self.risk_gate = risk_gate
        self.system_state = system_state
        self.account_lookup = account_lookup
        self.broker_timeout_seconds = broker_timeout_seconds
        self.unknown_poll_attempts = unknown_poll_attempts

    async def submit_order_intent(self, request: OrderRequest) -> ExecutionResult:
        if self.system_state.is_halted:
            return ExecutionResult(
                order_intent_id=request.order_intent_id,
                state=OrderExecutionState.FAILED,
                risk_check=RiskCheckResult(
                    passed=False,
                    stage="system",
                    reason="system is halted",
                ),
                ack=None,
                message="system halted",
            )

        reserved = await self.idempotency.try_reserve_async(request)
        if reserved is None:
            return ExecutionResult(
                order_intent_id=request.order_intent_id,
                state=OrderExecutionState.UNKNOWN_SUBMITTED,
                risk_check=RiskCheckResult(passed=True, stage="idempotency"),
                ack=None,
                message="blocked by active idempotency duplicate rule",
            )

        state = self.state_machine.next(None, OrderExecutionEvent.SIGNAL_CREATED)
        account = self.account_lookup(request.account_id)
        risk = await self.risk_gate.evaluate(request, account)
        if not risk.passed:
            await self.idempotency.mark_finalized_async(request, "RISK_REJECTED")
            return ExecutionResult(
                request.order_intent_id,
                self.state_machine.next(state, OrderExecutionEvent.RISK_REJECTED),
                risk,
                None,
                "risk_gate_blocked",
            )

        state = self.state_machine.next(state, OrderExecutionEvent.RISK_APPROVED)
        state = self.state_machine.next(state, OrderExecutionEvent.ORDER_SUBMIT)

        try:
            ack = await asyncio.wait_for(
                self.broker.submit_order(request),
                timeout=self.broker_timeout_seconds,
            )
        except asyncio.TimeoutError:
            state = self.state_machine.next(state, OrderExecutionEvent.UNKNOWN_SUBMITTED)
            await self.idempotency.mark_unknown_submitted_async(request)
            recovered = await self._poll_unknown_status(request.order_intent_id)
            if recovered is not None:
                state = self.state_machine.next(OrderExecutionState.UNKNOWN_SUBMITTED, OrderExecutionEvent.BROKER_FOUND)
                state = self._advance_from_ack(state, recovered)
                await self.idempotency.mark_finalized_async(request, recovered.status.value)
                return ExecutionResult(request.order_intent_id, state, risk, recovered, "recovered")

            return ExecutionResult(
                request.order_intent_id,
                state,
                risk,
                None,
                "unknown_submitted",
            )

        if ack is None:
            state = self.state_machine.next(state, OrderExecutionEvent.UNKNOWN_SUBMITTED)
            await self.idempotency.mark_unknown_submitted_async(request)
            return ExecutionResult(
                request.order_intent_id,
                state,
                risk,
                None,
                "ack_none_as_unknown",
            )

        if ack.is_rejected:
            state = self.state_machine.next(state, OrderExecutionEvent.BROKER_REJECTED)
            await self.idempotency.mark_finalized_async(request, "BROKER_REJECTED")
            return ExecutionResult(request.order_intent_id, state, risk, ack, "rejected")

        state = self.state_machine.next(state, OrderExecutionEvent.BROKER_ACK)
        state = self._advance_from_ack(state, ack)
        await self.idempotency.mark_finalized_async(request, ack.status.value)
        return ExecutionResult(request.order_intent_id, state, risk, ack, "completed")

    async def _poll_unknown_status(self, order_intent_id: str) -> OrderAck | None:
        delay = 0.25
        for attempt in range(self.unknown_poll_attempts):
            status = await self.broker.get_order_status(order_intent_id)
            if status is not None:
                return status
            await asyncio.sleep(delay)
            delay *= 2
        return None

    def _advance_from_ack(self, current_state: OrderExecutionState, ack: OrderAck) -> OrderExecutionState:
        if current_state == OrderExecutionState.BROKER_STATUS_QUERYING:
            current_state = self.state_machine.next(current_state, OrderExecutionEvent.BROKER_ACK)

        if ack.status == OrderStatus.FILLED:
            current_state = self.state_machine.next(current_state, OrderExecutionEvent.BROKER_FILLED)
            return self.state_machine.next(current_state, OrderExecutionEvent.RECONCILE)
        if ack.status == OrderStatus.PARTIALLY_FILLED:
            current_state = self.state_machine.next(current_state, OrderExecutionEvent.BROKER_PARTIAL_FILL)
            # fill remains open in this skeleton, so keep partial state as-is
            return current_state
        return current_state
