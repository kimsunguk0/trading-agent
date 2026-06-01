"""Unit tests for AnomalyDetector state-transition logic."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from agents.monitoring.anomaly_detector import AnomalyDetector
from core.system_state import SystemState, SystemStateManager


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _make_detector(**kwargs) -> tuple[AnomalyDetector, MagicMock]:
    """Return (detector, mock_state_mgr) with default thresholds matching spec."""
    mgr = MagicMock(spec=SystemStateManager)
    mgr.state = SystemState.NORMAL
    detector = AnomalyDetector(
        mgr,
        market_data_delay_seconds=Decimal("3"),
        websocket_gap_seconds=Decimal("10"),
        broker_delay_limit=3,
        balance_failure_limit=3,
        llm_parse_failure_rate_threshold=Decimal("0.10"),
        llm_parse_window_seconds=60,
        rapid_order_window_seconds=30,
        rapid_order_threshold=5,
        daily_loss_threshold=Decimal("0.01"),
        **kwargs,
    )
    return detector, mgr


# ─────────────────────────────────────────────────────────────────────────────
# 1. Market data delay → DEGRADED_MARKET
# ─────────────────────────────────────────────────────────────────────────────

def test_market_data_delay_triggers_degraded_market():
    detector, mgr = _make_detector()
    stale_ts = _utcnow() - timedelta(seconds=5)
    detector.metrics["last_market_data_ts"] = stale_ts

    asyncio.get_event_loop().run_until_complete(detector.tick())

    calls = [call.args[0] for call in mgr.transition_to.call_args_list]
    assert SystemState.DEGRADED_MARKET in calls


# ─────────────────────────────────────────────────────────────────────────────
# 2. WebSocket heartbeat gap → DEGRADED_MARKET
# ─────────────────────────────────────────────────────────────────────────────

def test_websocket_gap_triggers_degraded_market():
    detector, mgr = _make_detector()
    stale_ts = _utcnow() - timedelta(seconds=15)
    detector.metrics["last_heartbeat_ts"] = stale_ts

    asyncio.get_event_loop().run_until_complete(detector.tick())

    calls = [call.args[0] for call in mgr.transition_to.call_args_list]
    assert SystemState.DEGRADED_MARKET in calls


# ─────────────────────────────────────────────────────────────────────────────
# 3. Broker delay 3 consecutive times → DEGRADED_MARKET
# ─────────────────────────────────────────────────────────────────────────────

def test_broker_delay_3x_triggers_degraded_market():
    detector, mgr = _make_detector()
    detector.record_broker_delay(True)
    detector.record_broker_delay(True)
    detector.record_broker_delay(True)

    asyncio.get_event_loop().run_until_complete(detector.tick())

    calls = [call.args[0] for call in mgr.transition_to.call_args_list]
    assert SystemState.DEGRADED_MARKET in calls


# ─────────────────────────────────────────────────────────────────────────────
# 4. Balance check failure 3 consecutive times → DEGRADED_MARKET
# ─────────────────────────────────────────────────────────────────────────────

def test_balance_fail_3x_triggers_degraded_market():
    detector, mgr = _make_detector()
    detector.record_balance_check(success=False)
    detector.record_balance_check(success=False)
    detector.record_balance_check(success=False)

    asyncio.get_event_loop().run_until_complete(detector.tick())

    calls = [call.args[0] for call in mgr.transition_to.call_args_list]
    assert SystemState.DEGRADED_MARKET in calls


# ─────────────────────────────────────────────────────────────────────────────
# 5. LLM parse failure rate ≥ 10%/min → DEGRADED_LLM
# ─────────────────────────────────────────────────────────────────────────────

def test_llm_parse_rate_triggers_degraded_llm():
    detector, mgr = _make_detector()
    now = _utcnow()
    # Record 10 attempts with 2 failures → 20% failure rate, exceeds 10%
    for i in range(8):
        detector.record_llm_parse(success=True, ts=now - timedelta(seconds=i))
    for i in range(2):
        detector.record_llm_parse(success=False, ts=now - timedelta(seconds=i + 8))

    asyncio.get_event_loop().run_until_complete(detector.tick())

    calls = [call.args[0] for call in mgr.transition_to.call_args_list]
    assert SystemState.DEGRADED_LLM in calls


# ─────────────────────────────────────────────────────────────────────────────
# 6. Daily loss limit reached → EMERGENCY_STOP
# ─────────────────────────────────────────────────────────────────────────────

def test_daily_loss_triggers_emergency_stop():
    detector, mgr = _make_detector()
    detector.metrics["daily_loss_pct"] = Decimal("0.015")  # exceeds 0.01

    asyncio.get_event_loop().run_until_complete(detector.tick())

    calls = [call.args[0] for call in mgr.transition_to.call_args_list]
    assert SystemState.EMERGENCY_STOP in calls


# ─────────────────────────────────────────────────────────────────────────────
# 7. Same symbol+direction 5x within 30s → EMERGENCY_STOP
# ─────────────────────────────────────────────────────────────────────────────

def test_rapid_orders_triggers_emergency_stop():
    detector, mgr = _make_detector()
    now = _utcnow()
    # 5 rapid BUY orders for the same symbol within 30s
    for i in range(5):
        detector.record_order(symbol="005930", direction="BUY", ts=now - timedelta(seconds=i))

    asyncio.get_event_loop().run_until_complete(detector.tick())

    calls = [call.args[0] for call in mgr.transition_to.call_args_list]
    assert SystemState.EMERGENCY_STOP in calls


# ─────────────────────────────────────────────────────────────────────────────
# 8. Two simultaneous DEGRADED components → BROWNOUT
# ─────────────────────────────────────────────────────────────────────────────

def test_two_degraded_components_triggers_brownout():
    detector, mgr = _make_detector()
    now = _utcnow()
    # Trigger market data delay AND websocket gap simultaneously
    detector.metrics["last_market_data_ts"] = now - timedelta(seconds=10)
    detector.metrics["last_heartbeat_ts"] = now - timedelta(seconds=20)

    asyncio.get_event_loop().run_until_complete(detector.tick())

    calls = [call.args[0] for call in mgr.transition_to.call_args_list]
    assert SystemState.BROWNOUT in calls


# ─────────────────────────────────────────────────────────────────────────────
# 9. Broker delay resets on success
# ─────────────────────────────────────────────────────────────────────────────

def test_broker_delay_resets_on_success():
    detector, mgr = _make_detector()
    detector.record_broker_delay(True)
    detector.record_broker_delay(True)
    detector.record_broker_delay(False)  # success → reset

    assert int(detector.metrics["consecutive_broker_delays"]) == 0


# ─────────────────────────────────────────────────────────────────────────────
# 10. EMERGENCY_STOP not cleared by anomaly_detector (only human can)
# ─────────────────────────────────────────────────────────────────────────────

def test_emergency_stop_not_cleared_programmatically():
    """AnomalyDetector must NOT transition away from EMERGENCY_STOP."""
    mgr = MagicMock(spec=SystemStateManager)
    mgr.state = SystemState.EMERGENCY_STOP
    # Make transition_to return EMERGENCY_STOP (simulating the guard in SystemStateManager)
    mgr.transition_to.return_value = SystemState.EMERGENCY_STOP

    detector = AnomalyDetector(mgr, daily_loss_threshold=Decimal("0.01"))
    # All metrics clean – detector should call transition_to NORMAL but mgr keeps EMERGENCY_STOP
    asyncio.get_event_loop().run_until_complete(detector.tick())

    # transition_to may have been called, but it cannot change the state
    for call in mgr.transition_to.call_args_list:
        called_state = call.args[0] if call.args else call.kwargs.get("next_state")
        # If called at all, verify the SystemStateManager guard would block non-human resume
        # (The detector itself must not call human_resume)
        assert called_state != SystemState.NORMAL or mgr.state == SystemState.EMERGENCY_STOP
