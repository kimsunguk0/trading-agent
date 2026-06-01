from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from agents.monitoring.position_monitor import MonitoredPosition, PositionExitRule, PositionMonitor


class _Bus:
    def __init__(self) -> None:
        self.events = []

    async def publish(self, event) -> str:
        self.events.append(event)
        return "ok"


def test_position_monitor_creates_stop_loss_exit() -> None:
    monitor = PositionMonitor(
        schema="trading_paper",
        account_id="acc",
        bus=_Bus(),  # type: ignore[arg-type]
        rule=PositionExitRule(stop_loss_pct=Decimal("0.03"), take_profit_pct=Decimal("0.06")),
    )
    position = MonitoredPosition(
        account_id="acc",
        symbol="005930",
        quantity=Decimal("3"),
        average_price=Decimal("100"),
    )

    decision = monitor.evaluate(
        position,
        Decimal("96.5"),
        now=datetime(2026, 5, 29, tzinfo=timezone.utc),
    )

    assert decision is not None
    assert decision.reason == "STOP_LOSS"
    assert decision.request.side == "SELL"
    assert decision.request.quantity == Decimal("3")
    assert decision.order_intent_id == "OI-20260529-MON-005930-STOP_LOSS"


@pytest.mark.asyncio
async def test_position_monitor_run_once_publishes_take_profit_once(monkeypatch: pytest.MonkeyPatch) -> None:
    bus = _Bus()
    monitor = PositionMonitor(
        schema="trading_paper",
        account_id="acc",
        bus=bus,  # type: ignore[arg-type]
        rule=PositionExitRule(stop_loss_pct=Decimal("0.03"), take_profit_pct=Decimal("0.05")),
    )
    position = MonitoredPosition(
        account_id="acc",
        symbol="005930",
        quantity=Decimal("2"),
        average_price=Decimal("100"),
    )

    async def positions() -> list[MonitoredPosition]:
        return [position]

    async def price(symbol: str) -> Decimal:
        return Decimal("106")

    monkeypatch.setattr(monitor, "_fetch_open_positions", positions)
    monkeypatch.setattr(monitor, "_fetch_latest_market_price", price)

    first = await monitor.run_once()
    second = await monitor.run_once()

    assert [item.reason for item in first] == ["TAKE_PROFIT"]
    assert second == []
    assert len(bus.events) == 2
