from __future__ import annotations

import asyncio
import json
from collections import defaultdict, deque
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import apps.bot_telegram.__main__ as bot
from agents.decision.decision_engine import DecisionEngine
from core.events.schemas import EventType, SignalEvent
from core.models.market import Side, Symbol
from core.system_state import SystemState, SystemStateMachine
from core.trading_controls import _FALLBACK_CONTROLS, is_entry_allowed, set_trading_control


class _Message:
    def __init__(self) -> None:
        self.texts: list[str] = []

    async def reply_text(self, text: str) -> None:
        self.texts.append(text)


def _update(user_id: int = 123) -> SimpleNamespace:
    return SimpleNamespace(effective_user=SimpleNamespace(id=user_id), message=_Message())


def _context(*args: str) -> SimpleNamespace:
    return SimpleNamespace(args=list(args))


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "paper")
    monkeypatch.setenv("TELEGRAM_ALLOWED_USER_IDS", "123")
    bot._FALLBACK_STRATEGY_MODES.clear()
    bot._FALLBACK_RUNTIME_MODES.clear()
    _FALLBACK_CONTROLS.clear()


@pytest.mark.asyncio
async def test_status_gracefully_reports_without_database() -> None:
    update = _update()

    await bot.cmd_status(update, _context())

    text = update.message.texts[-1]
    assert "상태 요약:" in text
    assert "시스템: NORMAL" in text
    assert "잔고: 데이터 없음" in text


@pytest.mark.asyncio
async def test_halt_transitions_system_state_and_triggers_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    update = _update()
    published: list[tuple[str, dict]] = []

    async def _publish(action: str, payload: dict | None = None) -> bool:
        published.append((action, payload or {}))
        return True

    monkeypatch.setattr(bot, "_publish_control_event", _publish)

    await bot.cmd_halt(update, _context())

    assert "HALT 적용: 상태=HALTED" in update.message.texts[-1]
    assert published[-1][0] == "cancel_open_orders"
    assert published[-1][1]["state"] == "HALTED"


@pytest.mark.asyncio
async def test_resume_paper_human_resume_from_halted(monkeypatch: pytest.MonkeyPatch) -> None:
    manager = SystemStateMachine(initial=SystemState.HALTED)
    manager._manual_override = SystemState.HALTED

    async def _load() -> SystemStateMachine:
        return manager

    monkeypatch.setattr(bot, "_load_system_state_machine", _load)
    monkeypatch.setattr(bot, "_publish_control_event", AsyncMock(return_value=True))
    update = _update()

    await bot.cmd_resume_paper(update, _context())

    assert manager.state == SystemState.NORMAL
    assert bot._FALLBACK_RUNTIME_MODES["paper"] == "PAPER"
    assert "운영모드=PAPER" in update.message.texts[-1]


@pytest.mark.asyncio
async def test_promote_requires_confirm_and_applies_strategy_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bot, "_strategy_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(bot, "_history_ok_for_strategy", AsyncMock(return_value=True))
    monkeypatch.setattr(bot, "_publish_control_event", AsyncMock(return_value=True))
    update = _update()

    await bot.cmd_promote(update, _context("LIVE_AUTO", "--strategy", "news_alpha", "--confirm"))

    assert bot._FALLBACK_STRATEGY_MODES["news_alpha"] == ("LIVE_AUTO", True)
    assert "전략 승격 완료" in update.message.texts[-1]


@pytest.mark.asyncio
async def test_disable_strategy_sets_inactive(monkeypatch: pytest.MonkeyPatch) -> None:
    bot._FALLBACK_STRATEGY_MODES["news_alpha"] = ("PAPER", True)
    monkeypatch.setattr(bot, "_strategy_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(bot, "_publish_control_event", AsyncMock(return_value=True))
    update = _update()

    await bot.cmd_disable(update, _context("--strategy", "news_alpha"))

    assert bot._FALLBACK_STRATEGY_MODES["news_alpha"] == ("PAPER", False)
    assert "전략 비활성화 완료" in update.message.texts[-1]


@pytest.mark.asyncio
async def test_mode_set_live_auto_requires_confirm_then_persists() -> None:
    update = _update()

    await bot.cmd_mode(update, _context("set", "LIVE_AUTO"))
    assert "LIVE 계열 모드는 --confirm" in update.message.texts[-1]

    await bot.cmd_mode(update, _context("set", "LIVE_AUTO", "--confirm"))
    assert bot._FALLBACK_RUNTIME_MODES["paper"] == "LIVE_AUTO"
    assert "운영모드 변경 완료: LIVE_AUTO" in update.message.texts[-1]


@pytest.mark.asyncio
async def test_disable_symbol_blocks_decision_engine_new_entry() -> None:
    update = _update()

    await bot.cmd_disable_symbol(update, _context("005930"))
    allowed, reason = await is_entry_allowed("005930", "KR", environment="paper", redis_url=None)

    assert allowed is False
    assert "symbol:005930 blocked" in reason

    class _Bus:
        redis_url = None
        stream_prefix = "paper.events"
        _client = None
        _fallback_streams = defaultdict(deque)
        _fallback_subscribers = defaultdict(list)

        def __init__(self) -> None:
            self.published: list[object] = []

        async def publish(self, event: object) -> str:
            self.published.append(event)
            return "ok"

        def stream_name(self, event_type: str) -> str:
            return f"{self.stream_prefix}.{event_type}"

    bus = _Bus()
    engine = DecisionEngine(broker=SimpleNamespace(), bus=bus, account_id="default")
    signal = SignalEvent(
        event_type=EventType.SIGNAL,
        strategy_id="chatops",
        account_id="default",
        symbol=Symbol("005930"),
        side=Side.BUY,
        signal_score=Decimal("1"),
        payload={"market": "KR", "quantity": "1", "price": "100"},
    )

    result = await engine.process_signal(signal)

    assert result is None
    assert bus.published
    assert getattr(bus.published[-1], "stage") == "trading_control"


@pytest.mark.asyncio
async def test_disable_market_blocks_shared_entry_control() -> None:
    await set_trading_control("market", "US", blocked=True, reason="test", actor="tester", environment="paper", redis_url=None)

    allowed, reason = await is_entry_allowed("AAPL", "US", environment="paper", redis_url=None)

    assert allowed is False
    assert "market:US blocked" in reason


@pytest.mark.asyncio
async def test_candidate_listener_raw_stream_skips_unknown_and_malformed(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[tuple[int, str, str | None]] = []

    candidate = {
        "event_type": "news_candidate",
        "code": "005930",
        "symbol_name": "삼성전자",
        "strategy_id": "kr_news_breakout_v1",
        "confidence": "0.81",
        "regime": "bull_trend",
        "news_summary": "실적 전망 상향",
        "technical_summary": "장중 고점 돌파",
        "risk": {
            "position_pct": "1.7",
            "spread_pct": "0.08",
            "stop_loss_pct": "2.5",
            "time_stop_minutes": "45",
            "take_profit": [{"pct": "4.0"}, {"pct": "8.0"}],
        },
        "price": "78500",
        "quantity": "12",
        "order_intent_id": "OI-TEST-1",
    }
    enveloped_candidate = {
        "event_type": "signal",
        "strategy_id": "kr_news_breakout_v1",
        "account_id": "default",
        "symbol": "005930",
        "side": "BUY",
        "signal_score": "0.82",
        "payload": {**candidate, "code": "000660", "symbol_name": "SK하이닉스", "order_intent_id": "OI-TEST-2"},
    }

    class _FakeRedis:
        closed = False

        def __init__(self) -> None:
            self.calls = 0

        async def xread(self, *_args: object, **_kwargs: object) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
            self.calls += 1
            if self.calls > 1:
                raise asyncio.CancelledError()
            return [
                (
                    "paper.events.signal",
                    [
                        ("1-0", {"payload": "{not-json"}),
                        ("2-0", {"payload": json.dumps({"event_type": "news_analysis", "payload": {"code": "005930"}})}),
                        ("3-0", {"payload": json.dumps(candidate, ensure_ascii=False)}),
                        ("4-0", {"payload": json.dumps(enveloped_candidate, ensure_ascii=False)}),
                    ],
                )
            ]

        async def aclose(self) -> None:
            self.closed = True

    class _FakeBot:
        def __init__(self, token: str | None = None) -> None:
            self.token = token

        async def send_message(self, chat_id: int, text: str) -> None:
            sent.append((chat_id, text, self.token))

    fake_redis = _FakeRedis()
    monkeypatch.setattr(bot, "_redis_client", lambda: fake_redis)
    monkeypatch.setattr(bot, "Bot", _FakeBot)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")

    with pytest.raises(asyncio.CancelledError):
        await bot._candidate_listener()

    assert fake_redis.closed is True
    assert len(sent) == 2
    assert sent[0][0] == 123
    assert sent[0][2] == "test-token"
    assert "[매수 후보] KR 005930 삼성전자" in sent[0][1]
    assert "[매수 후보] KR 000660 SK하이닉스" in sent[1][1]
    assert all("news_analysis" not in text for _chat_id, text, _token in sent)
