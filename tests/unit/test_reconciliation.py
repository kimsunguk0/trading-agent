from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest
from unittest.mock import AsyncMock

import agents.monitoring.reconciliation as reconciliation
from agents.monitoring.reconciliation import ReconciliationMonitor
from core.models.portfolio import Position


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
    monitor._persist_position_snapshot.assert_awaited_once_with("005930", Decimal("1"), Decimal("1"))
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
    monitor._persist_position_snapshot.assert_awaited_once_with("005930", Decimal("1.5"), Decimal("1.5"))
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
    monitor._persist_position_snapshot.assert_awaited_once_with("005930", Decimal("3"), Decimal("3"))
    monitor._publish_emergency_stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_reconciliation_persists_broker_average_price(monkeypatch: pytest.MonkeyPatch) -> None:
    executed: list[tuple[str, tuple[object, ...]]] = []

    class _Conn:
        async def execute(self, query: str, *args: object) -> None:
            assert "market_price" not in query
            assert "unrealized_pnl" not in query
            executed.append((query, args))

        async def close(self) -> None:
            return None

    async def _connect(_dsn: str) -> _Conn:
        return _Conn()

    class _Broker:
        async def get_positions(self, _account_id: str) -> dict[str, dict[str, object]]:
            return {
                "005930": {
                    "symbol": "005930",
                    "name": "삼성전자",
                    "quantity": "1",
                    "average_price": "353750",
                    "current_price": "351500",
                    "unrealized_pnl": "-2250",
                    "realized_pnl": "1000",
                }
            }

    monkeypatch.setattr(reconciliation.asyncpg, "connect", _connect)
    monitor = ReconciliationMonitor(schema="trading_paper", dsn="postgres://test", broker=_Broker(), redis_url="redis://localhost:6379/0")
    monitor._fetch_internal_positions = AsyncMock(return_value={"005930": Decimal("0.5")})  # type: ignore[method-assign]
    monitor._publish_reconciliation_log = AsyncMock()

    records = await monitor.check_once()

    assert records[0]["severity"] == "warning"
    snapshot_call = next(item for item in executed if "position_snapshots" in item[0])
    args = snapshot_call[1]
    assert args[1] == "005930"
    assert args[2] == "1"
    assert args[3] == "353750"
    assert args[4] == "1000"


@pytest.mark.asyncio
async def test_reconciliation_syncs_broker_average_price_when_quantity_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    executed: list[tuple[str, tuple[object, ...]]] = []

    class _Conn:
        async def execute(self, query: str, *args: object) -> None:
            assert "market_price" not in query
            assert "unrealized_pnl" not in query
            executed.append((query, args))

        async def close(self) -> None:
            return None

    async def _connect(_dsn: str) -> _Conn:
        return _Conn()

    class _Broker:
        async def get_positions(self, _account_id: str) -> dict[str, dict[str, object]]:
            return {
                "005930": {
                    "symbol": "005930",
                    "quantity": "1",
                    "average_price": "343000",
                }
            }

    monkeypatch.setattr(reconciliation.asyncpg, "connect", _connect)
    monitor = ReconciliationMonitor(schema="trading_paper", dsn="postgres://test", broker=_Broker(), redis_url="redis://localhost:6379/0")
    monitor._fetch_internal_positions = AsyncMock(return_value={"005930": Decimal("1")})  # type: ignore[method-assign]
    monitor._publish_reconciliation_log = AsyncMock()

    records = await monitor.check_once()

    assert records[0]["severity"] == "ok"
    snapshot_call = next(item for item in executed if "position_snapshots" in item[0])
    args = snapshot_call[1]
    assert args[1] == "005930"
    assert args[2] == "1"
    assert args[3] == "343000"
    assert args[4] == "0"


@pytest.mark.asyncio
async def test_reconciliation_simulated_position_mapping_preserves_average_price() -> None:
    broker = SimpleNamespace(
        _positions={
            ("default", "005930"): Position(
                account_id="default",
                symbol="005930",
                quantity=Decimal("2"),
                average_price=Decimal("100"),
            )
        }
    )
    monitor = ReconciliationMonitor(schema="trading_paper", dsn=None, broker=broker, redis_url="redis://localhost:6379/0")

    positions = await monitor._fetch_broker_positions()

    assert positions["005930"]["quantity"] == Decimal("2")
    assert positions["005930"]["average_price"] == Decimal("100")
