from __future__ import annotations

import pytest

from core.execution.state_machine import OrderExecutionEvent, OrderExecutionState, OrderStateMachine


def test_state_transition_to_reconciled() -> None:
    sm = OrderStateMachine()
    state = sm.next(None, OrderExecutionEvent.SIGNAL_CREATED)
    state = sm.next(state, OrderExecutionEvent.RISK_APPROVED)
    state = sm.next(state, OrderExecutionEvent.ORDER_SUBMIT)
    state = sm.next(state, OrderExecutionEvent.BROKER_ACK)
    state = sm.next(state, OrderExecutionEvent.BROKER_FILLED)
    state = sm.next(state, OrderExecutionEvent.RECONCILE)
    assert state == OrderExecutionState.RECONCILED


def test_state_machine_reject_from_signal() -> None:
    sm = OrderStateMachine()
    state = sm.next(None, OrderExecutionEvent.SIGNAL_CREATED)
    state = sm.next(state, OrderExecutionEvent.RISK_REJECTED)
    assert state == OrderExecutionState.RISK_REJECTED


def test_unknown_submitted_branch_and_block() -> None:
    sm = OrderStateMachine()
    state = sm.next(None, OrderExecutionEvent.SIGNAL_CREATED)
    state = sm.next(state, OrderExecutionEvent.RISK_APPROVED)
    state = sm.next(state, OrderExecutionEvent.ORDER_SUBMIT)
    state = sm.next(state, OrderExecutionEvent.UNKNOWN_SUBMITTED)
    assert state == OrderExecutionState.UNKNOWN_SUBMITTED


def test_invalid_transition_raises() -> None:
    sm = OrderStateMachine()
    with pytest.raises(ValueError):
        sm.next(OrderExecutionState.SIGNAL_CREATED, OrderExecutionEvent.BROKER_FILLED)


def test_live_approval_flow_to_submit() -> None:
    sm = OrderStateMachine()
    state = sm.next(None, OrderExecutionEvent.SIGNAL_CREATED)
    state = sm.next(state, OrderExecutionEvent.RISK_APPROVED)
    state = sm.next(state, OrderExecutionEvent.MANUAL_APPROVAL_REQUIRED)
    assert state == OrderExecutionState.MANUAL_APPROVAL_PENDING
    state = sm.next(state, OrderExecutionEvent.MANUAL_APPROVED)
    assert state == OrderExecutionState.APPROVED
    state = sm.next(state, OrderExecutionEvent.ORDER_SUBMIT)
    assert state == OrderExecutionState.ORDER_SUBMITTING


def test_partial_fill_cancel_remainder_reconciles() -> None:
    sm = OrderStateMachine()
    state = sm.next(None, OrderExecutionEvent.SIGNAL_CREATED)
    state = sm.next(state, OrderExecutionEvent.RISK_APPROVED)
    state = sm.next(state, OrderExecutionEvent.ORDER_SUBMIT)
    state = sm.next(state, OrderExecutionEvent.BROKER_ACK)
    state = sm.next(state, OrderExecutionEvent.BROKER_PARTIAL_FILL)
    state = sm.next(state, OrderExecutionEvent.CANCEL_REQUESTED)
    assert state == OrderExecutionState.CANCELED
    state = sm.next(state, OrderExecutionEvent.RECONCILE)
    assert state == OrderExecutionState.RECONCILED
