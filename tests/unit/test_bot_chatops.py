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


def _candidate_payload(code: str, name: str, order_intent_id: str) -> dict[str, object]:
    return {
        "event_type": "news_candidate",
        "code": code,
        "symbol_name": name,
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
        "order_intent_id": order_intent_id,
    }


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
    assert "📊 상태 요약" in text
    assert "시스템: NORMAL" in text
    assert "💰 계좌: 데이터 없음" in text


@pytest.mark.asyncio
async def test_halt_transitions_system_state_and_triggers_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    update = _update()
    published: list[tuple[str, dict]] = []

    async def _publish(action: str, payload: dict | None = None) -> bool:
        published.append((action, payload or {}))
        return True

    monkeypatch.setattr(bot, "_publish_control_event", _publish)

    await bot.cmd_halt(update, _context())

    assert "🛑 HALT 적용" in update.message.texts[-1]
    assert "상태: HALTED" in update.message.texts[-1]
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
    assert "운영모드: PAPER" in update.message.texts[-1]


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
    assert "LIVE 계열 모드 변경은 --confirm" in update.message.texts[-1]

    await bot.cmd_mode(update, _context("set", "LIVE_AUTO", "--confirm"))
    assert bot._FALLBACK_RUNTIME_MODES["paper"] == "LIVE_AUTO"
    assert "✅ 운영모드 변경 완료" in update.message.texts[-1]
    assert "현재: LIVE_AUTO" in update.message.texts[-1]


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

    candidate = _candidate_payload("005930", "삼성전자", "OI-TEST-1")
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
        values = {bot._candidate_cursor_key(): "2-0"}
        dedup: set[str] = set()

        def __init__(self) -> None:
            self.calls = 0

        async def get(self, key: str) -> str | None:
            return self.values.get(key)

        async def set(self, key: str, value: str) -> None:
            self.values[key] = value

        async def sadd(self, key: str, value: str) -> int:
            if value in self.dedup:
                return 0
            self.dedup.add(value)
            return 1

        async def expire(self, _key: str, _seconds: int) -> None:
            return None

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
    assert "🚨 매수 후보  삼성전자(005930)" in sent[0][1]
    assert "💸 제안: 지정가 78,500원 × 12주" in sent[0][1]
    assert "🚨 매수 후보  SK하이닉스(000660)" in sent[1][1]
    assert all("news_analysis" not in text for _chat_id, text, _token in sent)


@pytest.mark.asyncio
async def test_candidate_listener_initializes_cursor_at_stream_tail(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[str] = []
    old_candidate = _candidate_payload("005930", "삼성전자", "OI-OLD")
    new_candidate = _candidate_payload("000660", "SK하이닉스", "OI-NEW")

    class _FakeRedis:
        def __init__(self) -> None:
            self.values: dict[str, str] = {}
            self.dedup: set[str] = set()
            self.xread_cursors: list[str] = []
            self.closed = False
            self.calls = 0

        async def get(self, key: str) -> str | None:
            return self.values.get(key)

        async def set(self, key: str, value: str) -> None:
            self.values[key] = value

        async def xrevrange(self, _stream: str, count: int = 1) -> list[tuple[str, dict[str, str]]]:
            assert count == 1
            return [("4-0", {"payload": json.dumps(old_candidate, ensure_ascii=False)})]

        async def xread(self, streams: dict[str, str], *_args: object, **_kwargs: object) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
            self.calls += 1
            cursor = next(iter(streams.values()))
            self.xread_cursors.append(cursor)
            if self.calls > 1:
                raise asyncio.CancelledError()
            assert cursor == "4-0"
            return [("paper.events.signal", [("5-0", {"payload": json.dumps(new_candidate, ensure_ascii=False)})])]

        async def sadd(self, _key: str, value: str) -> int:
            if value in self.dedup:
                return 0
            self.dedup.add(value)
            return 1

        async def expire(self, _key: str, _seconds: int) -> None:
            return None

        async def aclose(self) -> None:
            self.closed = True

    class _FakeBot:
        def __init__(self, token: str | None = None) -> None:
            self.token = token

        async def send_message(self, chat_id: int, text: str) -> None:
            sent.append(text)

    fake_redis = _FakeRedis()
    monkeypatch.setattr(bot, "_redis_client", lambda: fake_redis)
    monkeypatch.setattr(bot, "Bot", _FakeBot)

    with pytest.raises(asyncio.CancelledError):
        await bot._candidate_listener()

    assert fake_redis.closed is True
    assert fake_redis.xread_cursors[0] == "4-0"
    assert fake_redis.values[bot._candidate_cursor_key()] == "5-0"
    assert len(sent) == 1
    assert "SK하이닉스" in sent[0]
    assert "삼성전자" not in sent[0]


@pytest.mark.asyncio
async def test_candidate_listener_resume_cursor_and_deduplicates_order_intent(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[str] = []
    duplicate = _candidate_payload("005930", "삼성전자", "OI-DUP")
    fresh = _candidate_payload("000660", "SK하이닉스", "OI-FRESH")

    class _FakeRedis:
        def __init__(self) -> None:
            self.values = {bot._candidate_cursor_key(): "5-0"}
            self.dedup = {"order_intent_id:OI-DUP"}
            self.xrevrange_called = False
            self.xread_cursors: list[str] = []
            self.closed = False
            self.calls = 0

        async def get(self, key: str) -> str | None:
            return self.values.get(key)

        async def set(self, key: str, value: str) -> None:
            self.values[key] = value

        async def xrevrange(self, *_args: object, **_kwargs: object) -> list[tuple[str, dict[str, str]]]:
            self.xrevrange_called = True
            return []

        async def xread(self, streams: dict[str, str], *_args: object, **_kwargs: object) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
            self.calls += 1
            cursor = next(iter(streams.values()))
            self.xread_cursors.append(cursor)
            if self.calls > 1:
                raise asyncio.CancelledError()
            assert cursor == "5-0"
            return [
                (
                    "paper.events.signal",
                    [
                        ("6-0", {"payload": json.dumps(duplicate, ensure_ascii=False)}),
                        ("7-0", {"payload": json.dumps(fresh, ensure_ascii=False)}),
                    ],
                )
            ]

        async def sadd(self, _key: str, value: str) -> int:
            if value in self.dedup:
                return 0
            self.dedup.add(value)
            return 1

        async def expire(self, _key: str, _seconds: int) -> None:
            return None

        async def aclose(self) -> None:
            self.closed = True

    class _FakeBot:
        def __init__(self, token: str | None = None) -> None:
            self.token = token

        async def send_message(self, chat_id: int, text: str) -> None:
            sent.append(text)

    fake_redis = _FakeRedis()
    monkeypatch.setattr(bot, "_redis_client", lambda: fake_redis)
    monkeypatch.setattr(bot, "Bot", _FakeBot)

    with pytest.raises(asyncio.CancelledError):
        await bot._candidate_listener()

    assert fake_redis.closed is True
    assert fake_redis.xrevrange_called is False
    assert fake_redis.xread_cursors[0] == "5-0"
    assert fake_redis.values[bot._candidate_cursor_key()] == "7-0"
    assert len(sent) == 1
    assert "SK하이닉스" in sent[0]
    assert "삼성전자" not in sent[0]
