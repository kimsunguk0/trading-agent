"""LLM news-analysis pipeline worker."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import asdict
from decimal import Decimal
from typing import Any

import redis.asyncio as redis
from pydantic import TypeAdapter

from agents.analysts.bear_case import BearCaseAnalyzer
from agents.analysts.catalyst_hunter import CatalystHunter
from agents.analysts.news_analyst import NewsAnalyst
from agents.analysts.verification import VerificationAgent
from core.events.schemas import NewsEvent


def _normalize_payload(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _normalize_payload(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_payload(item) for item in value]
    return value


async def _ensure_consumer_group(client: redis.Redis, stream: str, group: str) -> None:
    try:
        await client.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:  # pragma: no cover
        msg = str(exc)
        if "BUSYGROUP" not in msg and "already exists" not in msg:
            raise


async def _consume_news(
    client: redis.Redis,
    stream: str,
    group: str,
    consumer: str,
    target: str,
    analyst: NewsAnalyst,
    catalyst_hunter: CatalystHunter,
    bear_case_analyzer: BearCaseAnalyzer,
    verification_agent: VerificationAgent,
) -> None:
    adapter = TypeAdapter(NewsEvent)
    while True:
        entries = await client.xreadgroup(
            group,
            consumer,
            {stream: ">"},
            count=16,
            block=2000,
        )
        if not entries:
            continue

        for _stream, messages in entries:
            for _message_id, fields in messages:
                raw = fields.get("payload") if isinstance(fields, dict) else None
                if not isinstance(raw, str):
                    continue
                try:
                    payload = json.loads(raw)
                except Exception:
                    continue
                try:
                    event = adapter.validate_python(payload)
                except Exception:
                    continue

                analysis = await analyst.analyze(event)
                symbol = ""
                if analysis.symbol_candidates:
                    symbol = analysis.symbol_candidates[0].code

                catalyst = await catalyst_hunter.analyze(event, analysis)
                bear_case = await bear_case_analyzer.assess(event, analysis, catalyst)
                verification = await verification_agent.assess(event, symbol=symbol)

                body_hash = str(
                    event.payload.get("body_hash") if isinstance(event.payload, dict) else str(payload.get("body_hash", ""))
                )
                if not body_hash:
                    body_hash = f"{event.title}|{event.body}"
                analysis_dump = analysis.model_dump(mode="json")
                symbol_name = ""
                if analysis.symbol_candidates:
                    symbol_name = str(analysis.symbol_candidates[0].name)

                signal_payload = {
                    "event_type": "news_analysis",
                    "source": event.source,
                    "body_hash": body_hash,
                    "news_time": event.occurred_at.isoformat(),
                    "symbol_name": symbol_name,
                    "analysis": analysis_dump,
                    "catalyst": asdict(catalyst),
                    "bear_case": asdict(bear_case),
                    "verification": asdict(verification),
                }

                await client.xadd(
                    target,
                    {"payload": json.dumps(_normalize_payload(signal_payload), ensure_ascii=False)},
                )


async def main() -> None:
    environment = os.getenv("ENVIRONMENT", "paper").lower()
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    stream_prefix = os.getenv("REDIS_STREAM_PREFIX", f"{environment}.events")

    input_stream = f"{stream_prefix}.news"
    output_stream = f"{stream_prefix}.signals"
    news_group = os.getenv("WORKER_AGENT_GROUP", "worker_agent_news")
    news_consumer = os.getenv("WORKER_AGENT_CONSUMER", "worker_agent_0")

    client = redis.from_url(redis_url, decode_responses=True)
    await _ensure_consumer_group(client, input_stream, news_group)

    analyst = NewsAnalyst(
        environment=environment,
        redis_url=redis_url,
        llm_api_url=os.getenv("LLM_API_URL", "http://localhost:8000/v1"),
        llm_model=os.getenv("LLM_MODEL", "Qwen/Qwen3.6-35B-A3B"),
        llm_fallback_model=os.getenv("LLM_FALLBACK_MODEL", "claude-haiku-4-5-20251001"),
        cache_ttl_seconds=3600,
    )
    catalyst_hunter = CatalystHunter()
    bear_case_analyzer = BearCaseAnalyzer(schema=f"trading_{environment}")
    verification_agent = VerificationAgent(schema=f"trading_{environment}")

    async def run_forever() -> None:
        while True:
            await _consume_news(
                client=client,
                stream=input_stream,
                group=news_group,
                consumer=news_consumer,
                target=output_stream,
                analyst=analyst,
                catalyst_hunter=catalyst_hunter,
                bear_case_analyzer=bear_case_analyzer,
                verification_agent=verification_agent,
            )

    try:
        await run_forever()
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
