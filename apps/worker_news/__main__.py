"""News collector worker entrypoint."""

from __future__ import annotations

import asyncio
import logging
import os

from agents.collectors.news_collector import NewsCollector


logger = logging.getLogger(__name__)


def _log_level() -> str:
    return os.getenv("LOG_LEVEL", "INFO").upper()


async def main() -> None:
    logging.basicConfig(level=_log_level(), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    environment = os.getenv("ENVIRONMENT", "paper").lower()
    worker_name = os.getenv("WORKER_NAME", "worker-news")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    stream_prefix = os.getenv("REDIS_STREAM_PREFIX", f"{environment}.events")
    poll_interval = int(os.getenv("NEWS_POLL_INTERVAL_SECONDS", "300"))

    logger.info(
        "Starting news worker.",
        extra={
            "worker_name": worker_name,
            "environment": environment,
            "redis_prefix": stream_prefix,
            "poll_interval_seconds": poll_interval,
        },
    )
    collector = NewsCollector(
        environment=environment,
        redis_url=redis_url,
        redis_prefix=stream_prefix,
        poll_interval_seconds=poll_interval,
        dsn=os.getenv("DATABASE_URL"),
    )
    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())
