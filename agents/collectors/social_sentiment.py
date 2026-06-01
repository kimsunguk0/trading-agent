"""Social sentiment collector placeholder."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from core.events.bus import RedisStreamBus

POLL_INTERVAL_SECONDS = 300


# 데이터 소스 TBD:
# - Reddit API (Pushshift / official API)
# - 네이버 카페/커뮤니티 RSS
# - 카카오톡 오픈채팅(채널 공개방, 법적 허용 범위 내만)


@dataclass
class SocialSentiment:
    source: str
    symbol: str
    score: float
    published_at: datetime
    raw: dict[str, Any]


class SocialSentimentCollector:
    def __init__(
        self,
        environment: str = "paper",
        redis_url: str = "redis://localhost:6379/0",
        redis_prefix: str = "paper.events",
    ) -> None:
        self.environment = environment
        self.redis_url = redis_url
        self.redis_prefix = redis_prefix
        self.bus = RedisStreamBus(redis_url=redis_url, stream_prefix=redis_prefix)

    async def collect_once(self) -> list[SocialSentiment]:
        """Collect one batch of social sentiment candidates."""
        raise NotImplementedError

    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError

    async def run_forever(self) -> None:
        raise NotImplementedError

    async def publish(self, payload: SocialSentiment) -> None:
        raise NotImplementedError
