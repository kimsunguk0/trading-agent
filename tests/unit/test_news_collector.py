from __future__ import annotations

import sys
from types import ModuleType
from types import SimpleNamespace

import pytest

if "aiohttp" not in sys.modules:
    class _FakeClientSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
            return None

    sys.modules["aiohttp"] = SimpleNamespace(ClientSession=_FakeClientSession)

if "langdetect" not in sys.modules:
    fake_langdetect = ModuleType("langdetect")
    fake_langdetect.detect = lambda _text: "ko"  # type: ignore[attr-defined]
    fake_exception_module = ModuleType("langdetect.lang_detect_exception")

    class _FakeLangDetectException(Exception):
        pass

    fake_exception_module.LangDetectException = _FakeLangDetectException  # type: ignore[attr-defined]
    sys.modules["langdetect"] = fake_langdetect
    sys.modules["langdetect.lang_detect_exception"] = fake_exception_module

from agents.collectors.news_collector import NewsCollector
from core.events.schemas import EventType


RSS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <item>
      <title>삼성전자 실적 개선 기대</title>
      <link>https://example.com/news/1</link>
      <description>반도체 업황 회복으로 실적 개선 기대가 커지고 있다.</description>
      <pubDate>Mon, 08 Jun 2026 09:00:00 +0900</pubDate>
    </item>
  </channel>
</rss>
"""


class FakeBus:
    def __init__(self) -> None:
        self.events = []

    async def publish(self, event) -> str:  # noqa: ANN001
        self.events.append(event)
        return "0-1"


def _enable_only_naver(monkeypatch: pytest.MonkeyPatch) -> None:
    toggles = {
        "NEWS_ENABLE_RSS": "1",
        "NEWS_ENABLE_NAVER": "1",
        "NEWS_ENABLE_HANKYUNG": "0",
        "NEWS_ENABLE_YONHAP": "0",
        "NEWS_ENABLE_BLOOMBERG": "0",
        "NEWS_ENABLE_REUTERS": "0",
        "NEWS_ENABLE_DART": "0",
        "NEWS_ENABLE_EDGAR": "0",
    }
    for key, value in toggles.items():
        monkeypatch.setenv(key, value)


@pytest.mark.asyncio
async def test_news_collector_rss_parse_to_news_event(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_only_naver(monkeypatch)
    collector = NewsCollector(environment="paper", redis_prefix="paper.events", dsn=None)
    fake_bus = FakeBus()
    collector.bus = fake_bus  # type: ignore[assignment]

    async def fake_fetch(_self, _session, _url: str, *, params=None) -> str:  # noqa: ANN001
        assert params is None
        return RSS_XML

    monkeypatch.setattr(NewsCollector, "_fetch", fake_fetch)

    count = await collector.run_once()

    assert count == 1
    assert len(fake_bus.events) == 1
    event = fake_bus.events[0]
    assert event.event_type == EventType.NEWS
    assert event.title == "삼성전자 실적 개선 기대"
    assert event.body == "반도체 업황 회복으로 실적 개선 기대가 커지고 있다."
    assert event.source == "NAVER_FINANCE"
    assert event.payload["source_url"] == "https://example.com/news/1"
    assert event.payload["language"] in {"KR", "KO", "unknown"}
    assert event.payload["body_hash"]


@pytest.mark.asyncio
async def test_news_collector_source_failure_is_graceful(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_only_naver(monkeypatch)
    collector = NewsCollector(environment="paper", redis_prefix="paper.events", dsn=None)
    fake_bus = FakeBus()
    collector.bus = fake_bus  # type: ignore[assignment]

    async def failing_fetch(_self, _session, _url: str, *, params=None) -> str:  # noqa: ANN001
        raise RuntimeError("source unavailable")

    monkeypatch.setattr(NewsCollector, "_fetch", failing_fetch)

    count = await collector.run_once()

    assert count == 0
    assert fake_bus.events == []
