from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest
from unittest.mock import AsyncMock

from core.models.order import Fill
from core.events.schemas import EventType, FillEvent
from core.operating_mode import approve_order_intent, publish_live_approval_token, expire_approvals
import apps.bot_telegram.__main__ as bot
from core import operating_mode
from agents.monitoring import slippage_monitor
from agents.monitoring.reconciliation import ReconciliationMonitor


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.expiry: dict[str, datetime] = {}

    async def setex(self, key: str, seconds: int, value: str) -> None:
        self.values[key] = value
        self.expiry[key] = datetime.now(timezone.utc) + timedelta(seconds=seconds)

    async def get(self, key: str) -> str | None:
        expire = self.expiry.get(key)
        if expire is not None and datetime.now(timezone.utc) > expire:
            self.values.pop(key, None)
            self.expiry.pop(key, None)
            return None
        return self.values.get(key)

    async def keys(self, pattern: str) -> list[str]:
        return [key for key in self.values.keys() if key.endswith(pattern.split(":")[-1].replace("*", ""))]

    async def delete(self, key: str) -> None:
        self.values.pop(key, None)
        self.expiry.pop(key, None)

    async def pttl(self, key: str) -> int:
        exp = self.expiry.get(key)
        if exp is None:
            return -2
        delta = (exp - datetime.now(timezone.utc)).total_seconds()
        return max(int(delta * 1000), -2)

    async def xadd(self, *args, **kwargs) -> None:
        return None

    async def close(self) -> None:
        return None


def test_approval_token_ttl_120_seconds_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeRedis()
    base = datetime(2026, 5, 29, 9, 0, 0, tzinfo=timezone.utc)
    now = base

    def _fake_now() -> datetime:
        return now

    monkeypatch.setattr(operating_mode, "_now", _fake_now)
    monkeypatch.setattr(operating_mode, "_redis_client", lambda redis_url: fake)

    async def run() -> None:
        nonlocal now
        order_id = "OI-001"
        token = await publish_live_approval_token(order_id, environment="paper", ttl_seconds=120)
        assert token
        assert await operating_mode.get_pending_token(order_id, environment="paper") == token

        now = base + timedelta(seconds=121)
        expired = await expire_approvals(environment="paper")
        assert order_id in expired
        status = await operating_mode.order_intent_status(order_id, environment="paper")
        assert status == operating_mode.ApprovalStatus.EXPIRED.value

    import asyncio

    asyncio.run(run())


@pytest.mark.asyncio
async def test_approve_rejected_for_non_allowed_user(monkeypatch: pytest.MonkeyPatch) -> None:
    messages: list[str] = []

    class _Msg:
        async def reply_text(self, text: str) -> None:
            messages.append(text)

    class _User:
        id = 9876

    update = SimpleNamespace(effective_user=_User(), message=_Msg())
    context = SimpleNamespace(args=["OI-1"])

    monkeypatch.setenv("TELEGRAM_ALLOWED_USER_IDS", "12345")
    monkeypatch.setattr(bot, "approve_order_intent", AsyncMock())

    await bot.cmd_approve(update, context)

    assert messages == ["🔒 권한이 없습니다."]
    bot.approve_order_intent.assert_not_called()


@pytest.mark.asyncio
async def test_double_approve_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeRedis()
    base = datetime(2026, 5, 29, 9, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(operating_mode, "_now", lambda: base)
    monkeypatch.setattr(operating_mode, "_redis_client", lambda redis_url: fake)

    token = await publish_live_approval_token("OI-DOUBLE", environment="paper")

    first = await approve_order_intent("OI-DOUBLE", token, environment="paper")
    second = await approve_order_intent("OI-DOUBLE", token, environment="paper")

    assert first is True
    assert second is False


@pytest.mark.asyncio
async def test_slippage_exceed_threshold_triggers_alert(monkeypatch: pytest.MonkeyPatch) -> None:
    monitor = slippage_monitor.SlippageMonitor(
        stream_prefix="paper.events",
        schema="trading_paper",
        redis_url="redis://localhost:6379/0",
        dsn=None,
    )

    monkeypatch.setattr(
        monitor,
        "_fetch_candidate",
        AsyncMock(
            return_value=slippage_monitor._CandidateInfo(
                strategy_id="strat",
                intended_price=Decimal("100"),
                max_slippage_pct=Decimal("1"),
                symbol_market="KR",
                order_intent_id="OI-1",
                symbol_code="005930",
                side="BUY",
                quantity=Decimal("1"),
            )
        ),
    )

    alerts: list[tuple[str, str, Decimal, Decimal]] = []

    async def _fake_emit(strategy_id: str, order_intent_id: str, slippage_pct: Decimal, max_allowed: Decimal) -> None:
        alerts.append((strategy_id, order_intent_id, slippage_pct, max_allowed))

    monkeypatch.setattr(monitor, "_emit_slippage_alert", AsyncMock(side_effect=_fake_emit))

    fill = Fill(
        order_id="ORDER-1",
        order_intent_id="OI-1",
        quantity=Decimal("1"),
        price=Decimal("105"),
    )
    event = FillEvent(
        event_type=EventType.FILL,
        fill_id="F-1",
        fill=fill,
    )

    await monitor.handle_fill(event)
    assert len(alerts) == 1
    assert alerts[0][2] > alerts[0][3]


@pytest.mark.asyncio
async def test_position_mismatch_critical_publishes_emergency_stop() -> None:
    monitor = ReconciliationMonitor(schema="trading_paper", dsn=None, redis_url="redis://localhost:6379/0")

    monitor._fetch_internal_positions = AsyncMock(return_value={"005930": Decimal("1")})  # type: ignore[method-assign]
    monitor._fetch_broker_positions = AsyncMock(return_value={"005930": Decimal("3")})  # type: ignore[method-assign]
    monitor._publish_reconciliation_log = AsyncMock()  # type: ignore[method-assign]
    monitor._persist_position_snapshot = AsyncMock()  # type: ignore[method-assign]
    monitor._publish_emergency_stop = AsyncMock()  # type: ignore[method-assign]

    records = await monitor.check_once()
    assert len(records) == 1
    assert records[0]["severity"] == "critical"
    monitor._persist_position_snapshot.assert_awaited_once_with("005930", Decimal("3"), Decimal("3"))
    monitor._publish_emergency_stop.assert_awaited_once()
