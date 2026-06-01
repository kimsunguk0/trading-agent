from __future__ import annotations

from decimal import Decimal

import pytest
from unittest.mock import AsyncMock

from agents.monitoring.reconciliation import ReconciliationMonitor


@pytest.mark.asyncio
async def test_reconciliation_no_diff_is_ok() -> None:
    monitor = ReconciliationMonitor(schema="trading_paper", dsn=None, redis_url="redis://localhost:6379/0")
    monitor._fetch_internal_positions = AsyncMock(return_value={"005930": Decimal("1")})  # type: ignore[method-assign]
    monitor._fetch_broker_positions = AsyncMock(return_value={"005930": Decimal("1")})  # type: ignore[method-assign]
    monitor._publish_reconciliation_log = AsyncMock()
    monitor._persist_position_snapshot = AsyncMock()
    monitor._publish_emergency_stop = AsyncMock()

    records = await monitor.check_once()

    assert len(records) == 1
    assert records[0]["severity"] == "ok"
    monitor._persist_position_snapshot.assert_not_awaited()
    monitor._publish_emergency_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconciliation_warning_when_diff_less_than_one() -> None:
    monitor = ReconciliationMonitor(schema="trading_paper", dsn=None, redis_url="redis://localhost:6379/0")
    monitor._fetch_internal_positions = AsyncMock(return_value={"005930": Decimal("1")})  # type: ignore[method-assign]
    monitor._fetch_broker_positions = AsyncMock(return_value={"005930": Decimal("1.5")})  # type: ignore[method-assign]
    monitor._publish_reconciliation_log = AsyncMock()
    monitor._persist_position_snapshot = AsyncMock()
    monitor._publish_emergency_stop = AsyncMock()

    records = await monitor.check_once()

    assert len(records) == 1
    assert records[0]["severity"] == "warning"
    monitor._persist_position_snapshot.assert_awaited_once_with("005930", Decimal("1.5"))
    monitor._publish_emergency_stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconciliation_critical_when_diff_greater_or_equal_two() -> None:
    monitor = ReconciliationMonitor(schema="trading_paper", dsn=None, redis_url="redis://localhost:6379/0")
    monitor._fetch_internal_positions = AsyncMock(return_value={"005930": Decimal("1")})  # type: ignore[method-assign]
    monitor._fetch_broker_positions = AsyncMock(return_value={"005930": Decimal("3")})  # type: ignore[method-assign]
    monitor._publish_reconciliation_log = AsyncMock()
    monitor._persist_position_snapshot = AsyncMock()
    monitor._publish_emergency_stop = AsyncMock()

    records = await monitor.check_once()

    assert len(records) == 1
    assert records[0]["severity"] == "critical"
    monitor._persist_position_snapshot.assert_awaited_once_with("005930", Decimal("3"))
    monitor._publish_emergency_stop.assert_awaited_once()
