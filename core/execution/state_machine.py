"""Order execution state machine."""

from __future__ import annotations

from enum import Enum


class OrderExecutionState(str, Enum):
    SIGNAL_CREATED = "SIGNAL_CREATED"
    RISK_CHECK_PENDING = "RISK_CHECK_PENDING"
    MANUAL_APPROVAL_PENDING = "MANUAL_APPROVAL_PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    ORDER_SUBMITTING = "ORDER_SUBMITTING"
    BROKER_ACKED = "BROKER_ACKED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    RECONCILED = "RECONCILED"
    UNKNOWN_SUBMITTED = "UNKNOWN_SUBMITTED"
    BROKER_STATUS_QUERYING = "BROKER_STATUS_QUERYING"
    NOT_FOUND = "NOT_FOUND"
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
    RISK_REJECTED = "RISK_REJECTED"
    BROKER_REJECTED = "BROKER_REJECTED"
    FAILED = "FAILED"


class OrderExecutionEvent(str, Enum):
    SIGNAL_CREATED = "SIGNAL_CREATED"
    RISK_APPROVED = "RISK_APPROVED"
    RISK_REJECTED = "RISK_REJECTED"
    MANUAL_APPROVAL_REQUIRED = "MANUAL_APPROVAL_REQUIRED"
    MANUAL_APPROVED = "MANUAL_APPROVED"
    MANUAL_REJECTED = "MANUAL_REJECTED"
    MANUAL_EXPIRED = "MANUAL_EXPIRED"
    ORDER_SUBMIT = "ORDER_SUBMIT"
    BROKER_ACK = "BROKER_ACK"
    UNKNOWN_SUBMITTED = "UNKNOWN_SUBMITTED"
    BROKER_FOUND = "BROKER_FOUND"
    NOT_FOUND = "NOT_FOUND"
    MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
    BROKER_PARTIAL_FILL = "BROKER_PARTIAL_FILL"
    BROKER_FILLED = "BROKER_FILLED"
    BROKER_REJECTED = "BROKER_REJECTED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    BROKER_CANCELED = "BROKER_CANCELED"
    RECONCILE = "RECONCILE"


_transitions = {
    None: {OrderExecutionEvent.SIGNAL_CREATED: OrderExecutionState.SIGNAL_CREATED},
    OrderExecutionState.SIGNAL_CREATED: {
        OrderExecutionEvent.RISK_APPROVED: OrderExecutionState.RISK_CHECK_PENDING,
        OrderExecutionEvent.RISK_REJECTED: OrderExecutionState.RISK_REJECTED,
    },
    OrderExecutionState.RISK_CHECK_PENDING: {
        OrderExecutionEvent.MANUAL_APPROVAL_REQUIRED: OrderExecutionState.MANUAL_APPROVAL_PENDING,
        OrderExecutionEvent.ORDER_SUBMIT: OrderExecutionState.ORDER_SUBMITTING,
        OrderExecutionEvent.RISK_REJECTED: OrderExecutionState.RISK_REJECTED,
    },
    OrderExecutionState.MANUAL_APPROVAL_PENDING: {
        OrderExecutionEvent.MANUAL_APPROVED: OrderExecutionState.APPROVED,
        OrderExecutionEvent.MANUAL_REJECTED: OrderExecutionState.REJECTED,
        OrderExecutionEvent.MANUAL_EXPIRED: OrderExecutionState.EXPIRED,
    },
    OrderExecutionState.APPROVED: {
        OrderExecutionEvent.ORDER_SUBMIT: OrderExecutionState.ORDER_SUBMITTING,
    },
    OrderExecutionState.REJECTED: {},
    OrderExecutionState.EXPIRED: {},
    OrderExecutionState.ORDER_SUBMITTING: {
        OrderExecutionEvent.BROKER_ACK: OrderExecutionState.BROKER_ACKED,
        OrderExecutionEvent.UNKNOWN_SUBMITTED: OrderExecutionState.UNKNOWN_SUBMITTED,
        OrderExecutionEvent.BROKER_REJECTED: OrderExecutionState.BROKER_REJECTED,
        OrderExecutionEvent.BROKER_CANCELED: OrderExecutionState.CANCELED,
    },
    OrderExecutionState.BROKER_ACKED: {
        OrderExecutionEvent.BROKER_PARTIAL_FILL: OrderExecutionState.PARTIALLY_FILLED,
        OrderExecutionEvent.BROKER_FILLED: OrderExecutionState.FILLED,
        OrderExecutionEvent.CANCEL_REQUESTED: OrderExecutionState.CANCELED,
        OrderExecutionEvent.BROKER_CANCELED: OrderExecutionState.CANCELED,
    },
    OrderExecutionState.PARTIALLY_FILLED: {
        OrderExecutionEvent.BROKER_FILLED: OrderExecutionState.FILLED,
        OrderExecutionEvent.BROKER_REJECTED: OrderExecutionState.BROKER_REJECTED,
        OrderExecutionEvent.CANCEL_REQUESTED: OrderExecutionState.CANCELED,
        OrderExecutionEvent.BROKER_CANCELED: OrderExecutionState.CANCELED,
    },
    OrderExecutionState.CANCELED: {
        OrderExecutionEvent.RECONCILE: OrderExecutionState.RECONCILED,
    },
    OrderExecutionState.FILLED: {
        OrderExecutionEvent.RECONCILE: OrderExecutionState.RECONCILED,
    },
    OrderExecutionState.RECONCILED: {},
    OrderExecutionState.UNKNOWN_SUBMITTED: {
        OrderExecutionEvent.BROKER_FOUND: OrderExecutionState.BROKER_STATUS_QUERYING,
        OrderExecutionEvent.NOT_FOUND: OrderExecutionState.NOT_FOUND,
        OrderExecutionEvent.MANUAL_REVIEW_REQUIRED: OrderExecutionState.MANUAL_REVIEW_REQUIRED,
    },
    OrderExecutionState.BROKER_STATUS_QUERYING: {
        OrderExecutionEvent.BROKER_ACK: OrderExecutionState.BROKER_ACKED,
        OrderExecutionEvent.NOT_FOUND: OrderExecutionState.NOT_FOUND,
        OrderExecutionEvent.MANUAL_REVIEW_REQUIRED: OrderExecutionState.MANUAL_REVIEW_REQUIRED,
        OrderExecutionEvent.BROKER_CANCELED: OrderExecutionState.CANCELED,
    },
}


class OrderStateMachine:
    def next(self, state: OrderExecutionState | None, event: OrderExecutionEvent) -> OrderExecutionState:
        if event not in _transitions.get(state, {}):
            raise ValueError(f"Invalid transition {state} --({event})-->")
        return _transitions[state][event]

    @staticmethod
    def is_terminal(state: OrderExecutionState) -> bool:
        return state in {
            OrderExecutionState.RECONCILED,
            OrderExecutionState.NOT_FOUND,
            OrderExecutionState.MANUAL_REVIEW_REQUIRED,
            OrderExecutionState.REJECTED,
            OrderExecutionState.EXPIRED,
            OrderExecutionState.BROKER_REJECTED,
            OrderExecutionState.RISK_REJECTED,
            OrderExecutionState.FAILED,
        }
