"""LLM wrapper for converting raw news into structured analysis."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator

from core.events.schemas import NewsEvent
from core.events.bus import RedisStreamBus

from openai import AsyncOpenAI

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None


logger = logging.getLogger(__name__)


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return Decimal("1") if value else Decimal("0")
    if isinstance(value, int):
        return Decimal(value)
    return Decimal(str(value))


class NewsSymbolCandidate(BaseModel):
    market: str
    code: str
    name: str
    confidence: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))


class NewsAnalystOutput(BaseModel):
    symbol_candidates: list[NewsSymbolCandidate]
    event_type: str
    sentiment: str
    sentiment_score: Decimal = Field(ge=Decimal("-1"), le=Decimal("1"))
    catalyst_score: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    time_sensitivity: str
    source_quality: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    summary: str = Field(max_length=500)
    bull_case: list[str] = Field(default_factory=list)
    bear_case: list[str] = Field(default_factory=list)
    required_checks: list[str] = Field(default_factory=list)
    should_trade_directly: bool = False

    @field_validator("sentiment_score", "catalyst_score", "source_quality", mode="before")
    @classmethod
    def _coerce_score(cls, value: Any) -> Decimal:
        if isinstance(value, str):
            quality = value.strip().lower().replace("-", "_").replace(" ", "_")
            quality_map = {
                "very_high": Decimal("1.0"),
                "high": Decimal("0.9"),
                "medium_high": Decimal("0.75"),
                "medium": Decimal("0.6"),
                "mid": Decimal("0.6"),
                "neutral": Decimal("0.5"),
                "medium_low": Decimal("0.45"),
                "low": Decimal("0.3"),
                "very_low": Decimal("0.1"),
            }
            if quality in quality_map:
                return quality_map[quality]
        return _to_decimal(value)

    @field_validator("bull_case", "bear_case", "required_checks", mode="before")
    @classmethod
    def _coerce_string_list(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        if isinstance(value, list):
            return [str(item) for item in value]
        return [str(value)]

    @field_validator("should_trade_directly")
    @classmethod
    def _force_false(cls, value: bool) -> bool:
        return False


@dataclass
class FailureTracker:
    outcomes: deque[tuple[float, bool]]
    window_seconds: int = 60

    def __init__(self, window_seconds: int = 60) -> None:
        self.window_seconds = window_seconds
        self.outcomes = deque()

    def add(self, success: bool) -> float:
        now = datetime.now(timezone.utc).timestamp()
        self.outcomes.append((now, bool(success)))
        cutoff = now - self.window_seconds
        while self.outcomes and self.outcomes[0][0] < cutoff:
            self.outcomes.popleft()
        return self.failure_rate()

    def failure_rate(self) -> float:
        if not self.outcomes:
            return 0.0
        total = len(self.outcomes)
        fails = sum(1 for _, success in self.outcomes if not success)
        return fails / float(total)


class NewsAnalyst:
    def __init__(
        self,
        environment: str = "paper",
        redis_url: str | None = "redis://localhost:6379/0",
        llm_api_url: str | None = None,
        llm_model: str | None = None,
        llm_fallback_model: str | None = None,
        cache_ttl_seconds: int = 3600,
    ) -> None:
        self.environment = environment
        self.redis_url = redis_url
        self.llm_model = llm_model or os.getenv("LLM_MODEL", "Qwen/Qwen3.6-35B-A3B")
        self.llm_fallback_model = llm_fallback_model or os.getenv("LLM_FALLBACK_MODEL", "claude-haiku-4-5-20251001")
        self.llm_api_url = llm_api_url or os.getenv("LLM_API_URL", "http://localhost:8000/v1")
        self.llm_fallback_api_url = os.getenv("LLM_FALLBACK_API_URL", "").strip()
        self.cache_ttl_seconds = cache_ttl_seconds
        self._tracker = FailureTracker(window_seconds=60)
        self._last_degraded_at: datetime | None = None
        self.bus = RedisStreamBus(redis_url=redis_url or "redis://localhost:6379/0", stream_prefix=f"{environment}.events")
        self.client = AsyncOpenAI(
            base_url=self.llm_api_url,
            api_key=os.getenv("OPENAI_API_KEY", "EMPTY"),
        )
        self.fallback_client = (
            AsyncOpenAI(
                base_url=self.llm_fallback_api_url,
                api_key=os.getenv("LLM_FALLBACK_API_KEY")
                or os.getenv("ANTHROPIC_API_KEY")
                or os.getenv("OPENAI_API_KEY", "EMPTY"),
            )
            if self.llm_fallback_api_url
            else self.client
        )
        if redis_url and redis is not None:
            self._redis = redis.from_url(redis_url, decode_responses=True)
        else:
            self._redis = None

    async def _cache_get(self, key: str) -> dict[str, Any] | None:
        if self._redis is None:
            return None
        raw = await self._redis.get(key)
        if raw is None:
            return None
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                return payload
        except Exception:
            return None
        return None

    async def _cache_set(self, key: str, payload: dict[str, Any], ttl: int | None = None) -> None:
        if self._redis is None:
            return
        await self._redis.set(key, json.dumps(payload, ensure_ascii=False), ex=ttl or self.cache_ttl_seconds)

    def _content_hash(self, event: NewsEvent) -> str:
        raw = f"{event.source}|{event.title}|{event.body}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _build_prompt(self, event: NewsEvent, mapped: list[dict[str, Any]]) -> str:
        payload = {
            "title": event.title,
            "body": event.body,
            "mapped_companies": mapped,
            "existing_symbol_hints": event.payload or {},
        }
        return (
            "You are a strict financial news analyzer.\n"
            "Return only JSON that matches the schema exactly.\n"
            "Fields:\n"
            "- symbol_candidates (market/code/name/confidence)\n"
            "- event_type, sentiment, sentiment_score, catalyst_score\n"
            "- time_sensitivity, source_quality, summary, bull_case, bear_case\n"
            "- required_checks, should_trade_directly\n"
            "- Must force should_trade_directly = false\n"
            + json.dumps(payload, ensure_ascii=False)
        )

    def _fallback_is_configured(self) -> bool:
        if not self.llm_fallback_model or self.llm_fallback_model == self.llm_model:
            return False
        return bool(self.llm_fallback_api_url or os.getenv("ANTHROPIC_API_KEY", "").strip())

    def _model_clients(self) -> list[tuple[str, Any]]:
        candidates: list[tuple[str, Any]] = [(self.llm_model, self.client)]
        if self._fallback_is_configured():
            candidates.append((self.llm_fallback_model, self.fallback_client))
        return candidates

    async def _call_model(self, prompt: str) -> tuple[dict[str, Any], int, int]:
        last_error: Exception | None = None
        for model, client in self._model_clients():
            try:
                response = await client.chat.completions.create(
                    model=model,
                    temperature=0,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are an analyst for Korean market news. Return strict JSON.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    response_format={"type": "json_object"},
                )
                content = response.choices[0].message.content or "{}"
                data = json.loads(content)
                usage = getattr(response, "usage", None)
                prompt_tokens = int(getattr(usage, "prompt_tokens", 0))
                completion_tokens = int(getattr(usage, "completion_tokens", 0))
                return data, prompt_tokens, completion_tokens
            except Exception as exc:  # pragma: no cover - network dependent
                last_error = exc
                continue
        if (
            last_error is not None
            and self.llm_fallback_model
            and self.llm_fallback_model != self.llm_model
            and not self._fallback_is_configured()
        ):
            raise RuntimeError(
                "LLM primary model failed; fallback model "
                f"{self.llm_fallback_model!r} was skipped because neither "
                "ANTHROPIC_API_KEY nor LLM_FALLBACK_API_URL is configured."
            ) from last_error
        raise last_error or RuntimeError("llm call failed")

    def _to_decimal(self, value: Any) -> Decimal:
        return _to_decimal(value)

    def _parse_output(self, payload: dict[str, Any]) -> NewsAnalystOutput:
        coerced = dict(payload)
        coerced["should_trade_directly"] = False
        return NewsAnalystOutput(**coerced)

    async def _record_run(
        self,
        *,
        event: NewsEvent,
        content_hash: str,
        input_refs: dict[str, Any],
        output_json: dict[str, Any],
        tokens_in: int,
        tokens_out: int,
        cost_usd: Decimal,
        cache_hit: bool,
        latency_ms: int,
        model_name: str,
    ) -> None:
        from uuid import uuid4

        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            return
        try:
            conn = await __import__("asyncpg").connect(dsn)
            await conn.execute(
                """
                INSERT INTO trading_{} .agent_runs (
                    id,
                    agent_name,
                    model_provider,
                    model_name,
                    prompt_version,
                    prompt_hash,
                    input_refs,
                    output_json,
                    tokens_in,
                    tokens_out,
                    cost_usd,
                    cache_hit,
                    latency_ms
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
                )
                """.format(self.environment),
                str(uuid4()),
                "news_analyst",
                "openai_compatible",
                model_name,
                "v1",
                self._content_hash(event),
                input_refs,
                output_json,
                tokens_in,
                tokens_out,
                cost_usd,
                cache_hit,
                latency_ms,
            )
            await conn.close()
        except Exception:
            return

    async def _publish_degraded_state(self) -> None:
        stream = f"{self.environment}.events.system_state"
        now = datetime.now(timezone.utc)
        if self._last_degraded_at is not None:
            if now - self._last_degraded_at < timedelta(minutes=1):
                return
        self._last_degraded_at = now
        if self._redis is not None:
            payload = {
                "event_type": "system_state",
                "state": "DEGRADED_LLM",
                "reason": "llm_failure_rate_over_10pct",
                "timestamp": now.isoformat(),
            }
            try:
                await self._redis.xadd(stream, {"payload": json.dumps(payload, ensure_ascii=False)})
            except Exception:
                pass

    def _cost(self, prompt_tokens: int, completion_tokens: int, *, provider: str = "primary") -> Decimal:
        # fallback default costs (USD)
        in_rate = Decimal(os.getenv("LLM_INPUT_COST_PER_1K", "0"))
        out_rate = Decimal(os.getenv("LLM_OUTPUT_COST_PER_1K", "0"))
        return (_to_decimal(prompt_tokens) * in_rate / Decimal("1000")) + (
            _to_decimal(completion_tokens) * out_rate / Decimal("1000")
        )

    async def analyze(self, event: NewsEvent, *, symbol_candidates: list[dict[str, Any]] | None = None) -> NewsAnalystOutput | None:
        start = datetime.now(timezone.utc)
        content_hash = self._content_hash(event)
        cache_key = f"news_analyst:{content_hash}"
        cached = await self._cache_get(cache_key)
        if isinstance(cached, dict):
            try:
                parsed = self._parse_output(cached)
            except Exception:
                logger.warning("Skipping invalid cached news analysis payload.", exc_info=True)
                parsed = None
        else:
            parsed = None

        if parsed is not None:
            await self._record_run(
                event=event,
                content_hash=content_hash,
                input_refs={"source": event.source, "content_hash": content_hash},
                output_json=parsed.model_dump(mode="json"),
                tokens_in=0,
                tokens_out=0,
                cost_usd=Decimal("0"),
                cache_hit=True,
                latency_ms=0,
                model_name=self.llm_model,
            )
            return parsed

        prompt = self._build_prompt(event, symbol_candidates or [])
        try:
            parsed_json, prompt_tokens, completion_tokens = await self._call_model(prompt)
        except Exception:
            self._tracker.add(False)
            if self._tracker.failure_rate() >= 0.10:
                await self._publish_degraded_state()
            raise

        try:
            parsed = self._parse_output(dict(parsed_json))
        except Exception:
            self._tracker.add(False)
            logger.warning(
                "Skipping news analysis with invalid LLM output.",
                extra={"news_event_id": event.event_id, "llm_output": parsed_json},
                exc_info=True,
            )
            if self._tracker.failure_rate() >= 0.10:
                await self._publish_degraded_state()
            return None

        try:
            await self._cache_set(cache_key, parsed.model_dump(mode="json"), self.cache_ttl_seconds)
            latency_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)
            await self._record_run(
                event=event,
                content_hash=content_hash,
                input_refs={"source": event.source, "content_hash": content_hash, "prompt": prompt},
                output_json=parsed.model_dump(mode="json"),
                tokens_in=prompt_tokens,
                tokens_out=completion_tokens,
                cost_usd=self._cost(prompt_tokens, completion_tokens),
                cache_hit=False,
                latency_ms=latency_ms,
                model_name=self.llm_model,
            )
            self._tracker.add(True)
            return parsed
        except Exception:
            self._tracker.add(False)
            if self._tracker.failure_rate() >= 0.10:
                await self._publish_degraded_state()
            raise


async def run() -> None:
    return
