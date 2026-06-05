from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from agents.analysts.news_analyst import NewsAnalyst, NewsAnalystOutput
from core.events.schemas import EventType, NewsEvent


def _make_news_event() -> NewsEvent:
    return NewsEvent(
        event_type=EventType.NEWS,
        title="매출 성장 기대",
        body="ABC 주가 상승 기대감이 큰 보도",
        source="TEST",
        occurred_at=datetime.now(timezone.utc),
        payload={"body_hash": str(uuid4())},
    )


def _llm_payload() -> dict:
    return {
        "symbol_candidates": [
            {"market": "KR", "code": "005930", "name": "삼성전자", "confidence": "0.97"}
        ],
        "event_type": "earnings",
        "sentiment": "positive",
        "sentiment_score": "0.82",
        "catalyst_score": "0.87",
        "time_sensitivity": "intraday",
        "source_quality": "high",
        "summary": "실적 개선 기대감이 반영된 긍정 뉴스",
        "bull_case": "매출 성장",
        "bear_case": "환율 부담",
        "required_checks": "공시 확인",
        "should_trade_directly": True,
    }


def test_news_analyst_output_coerces_qwen_schema_drift() -> None:
    result = NewsAnalystOutput(**_llm_payload())

    assert result.source_quality == Decimal("0.9")
    assert result.sentiment_score == Decimal("0.82")
    assert result.catalyst_score == Decimal("0.87")
    assert result.bull_case == ["매출 성장"]
    assert result.bear_case == ["환율 부담"]
    assert result.required_checks == ["공시 확인"]
    assert result.should_trade_directly is False


async def test_news_analyst_call_model_uses_vllm_compatible_json_object() -> None:
    calls: list[dict] = []

    class _Completions:
        async def create(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(content=json.dumps(_llm_payload(), ensure_ascii=False))
                    )
                ],
                usage=SimpleNamespace(prompt_tokens=10, completion_tokens=5),
            )

    analyst = NewsAnalyst(redis_url=None)
    analyst.client = SimpleNamespace(chat=SimpleNamespace(completions=_Completions()))

    data, prompt_tokens, completion_tokens = await analyst._call_model("prompt")

    assert data["source_quality"] == "high"
    assert prompt_tokens == 10
    assert completion_tokens == 5
    assert calls
    assert calls[0]["response_format"] == {"type": "json_object"}
    assert "extra_body" not in calls[0]


async def test_news_analyst_skips_nonlocal_fallback_when_anthropic_key_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("LLM_FALLBACK_API_URL", raising=False)
    calls: list[dict] = []

    class _Completions:
        async def create(self, **kwargs):
            calls.append(kwargs)
            raise RuntimeError("vllm 400")

    analyst = NewsAnalyst(
        redis_url=None,
        llm_model="Qwen3-30B-A3B-Instruct-2507",
        llm_fallback_model="claude-haiku-4-5-20251001",
    )
    analyst.client = SimpleNamespace(chat=SimpleNamespace(completions=_Completions()))
    analyst.fallback_client = analyst.client

    with pytest.raises(RuntimeError) as exc:
        await analyst._call_model("prompt")

    assert len(calls) == 1
    assert calls[0]["model"] == "Qwen3-30B-A3B-Instruct-2507"
    assert "fallback model 'claude-haiku-4-5-20251001' was skipped" in str(exc.value)


async def test_news_analyst_enforces_schema_and_forces_no_direct_trade() -> None:
    event = _make_news_event()
    analyst = NewsAnalyst(redis_url=None)
    analyst._cache_get = AsyncMock(return_value=None)
    analyst._cache_set = AsyncMock()
    analyst._record_run = AsyncMock()
    analyst._call_model = AsyncMock(
        return_value=(
            {
                "symbol_candidates": [
                    {"market": "KR", "code": "005930", "name": "삼성전자", "confidence": 0.97}
                ],
                "event_type": "earnings",
                "sentiment": "positive",
                "sentiment_score": 0.82,
                "catalyst_score": 0.87,
                "time_sensitivity": "intraday",
                "source_quality": 0.91,
                "summary": "실적 개선 기대감이 반영된 긍정 뉴스",
                "bull_case": ["매출 성장"],
                "bear_case": [],
                "required_checks": ["공시 확인"],
                "should_trade_directly": True,
            },
            100,
            50,
        )
    )

    result = await analyst.analyze(event)
    assert result.should_trade_directly is False
    assert result.symbol_candidates
    assert analyst._cache_set.await_count == 1
    assert analyst._record_run.await_count == 1


async def test_news_analyst_cache_hit_skips_llm() -> None:
    event = _make_news_event()
    cache: dict[str, dict] = {}

    async def cache_get(key: str) -> dict | None:
        return cache.get(key)

    async def cache_set(key: str, payload: dict, ttl: int | None = None) -> None:
        cache[key] = payload

    calls = {"llm": 0}

    async def call_model(_: str):
        calls["llm"] += 1
        return (
            {
                "symbol_candidates": [
                    {"market": "KR", "code": "005930", "name": "삼성전자", "confidence": 0.97}
                ],
                "event_type": "earnings",
                "sentiment": "positive",
                "sentiment_score": 0.82,
                "catalyst_score": 0.87,
                "time_sensitivity": "intraday",
                "source_quality": 0.91,
                "summary": "실적 개선 기대감이 반영된 긍정 뉴스",
                "bull_case": ["매출 성장"],
                "bear_case": [],
                "required_checks": ["공시 확인"],
                "should_trade_directly": False,
            },
            100,
            50,
        )

    analyst = NewsAnalyst(redis_url=None)
    analyst._cache_get = cache_get
    analyst._cache_set = cache_set
    analyst._record_run = AsyncMock()
    analyst._call_model = call_model

    first = await analyst.analyze(event)
    second = await analyst.analyze(event)

    assert calls["llm"] == 1
    assert first.should_trade_directly is False
    assert second.should_trade_directly is False


async def test_news_analyst_degraded_llm_emits_system_state() -> None:
    event = _make_news_event()
    published = []

    class FakeRedis:
        async def xadd(self, stream: str, payload: dict[str, str]) -> None:
            published.append((stream, payload))

    analyst = NewsAnalyst(redis_url="redis://localhost:6379/0", environment="paper")
    analyst._redis = FakeRedis()
    analyst._cache_get = AsyncMock(return_value=None)
    analyst._cache_set = AsyncMock()
    analyst._record_run = AsyncMock()
    analyst._call_model = AsyncMock(side_effect=RuntimeError("llm down"))

    try:
        await analyst.analyze(event)
    except RuntimeError:
        pass

    assert any(item[0] == "paper.events.system_state" for item in published)
    assert any("DEGRADED_LLM" in item[1].get("payload", "{}") for item in published)
