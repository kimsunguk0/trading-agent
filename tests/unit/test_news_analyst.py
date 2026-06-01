from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

from agents.analysts.news_analyst import NewsAnalyst
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
