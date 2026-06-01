"""Company name -> stock code resolver for KR symbols."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from openai import AsyncOpenAI


try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None


@dataclass
class ResolvedCompany:
    market: str
    code: str
    name: str
    confidence: Decimal


def _normalize_name(value: str) -> str:
    return value.strip().lower().replace(" ", "")


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


class EntityResolver:
    def __init__(
        self,
        universe_path: str = "data/universe/kr_instruments.json",
        redis_url: str | None = "redis://localhost:6379/0",
        model: str = "local-qwen-35b-a3b",
        ttl_seconds: int = 3600,
    ) -> None:
        self.universe_path = Path(universe_path)
        self.redis_url = redis_url
        self.model = model
        self.ttl_seconds = ttl_seconds
        self._client = AsyncOpenAI(
            base_url=os.getenv("LLM_API_URL", "http://localhost:8000/v1"),
            api_key=os.getenv("OPENAI_API_KEY", "EMPTY"),
        )
        self._mapping = self._load_mapping()
        if redis_url and redis is not None:
            self._redis = redis.from_url(redis_url, decode_responses=True)
        else:
            self._redis = None

    def _load_mapping(self) -> dict[str, str]:
        if not self.universe_path.exists():
            return {}

        try:
            raw = json.loads(self.universe_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

        items = raw.get("instruments", raw) if isinstance(raw, dict) else raw
        mapping: dict[str, str] = {}
        if not isinstance(items, list):
            return mapping

        for row in items:
            if not isinstance(row, dict):
                continue
            code = str(row.get("code") or row.get("symbol") or "").strip()
            name = str(row.get("name") or row.get("kor_name") or "").strip()
            if code and name:
                mapping[_normalize_name(name)] = code
        return mapping

    async def _cache_get(self, key: str) -> dict[str, Any] | None:
        if self._redis is None:
            return None
        raw = await self._redis.get(key)
        if raw is None:
            return None
        try:
            import json

            payload = json.loads(raw)
            if isinstance(payload, dict):
                return payload
        except Exception:
            return None
        return None

    async def _cache_set(self, key: str, payload: dict[str, Any]) -> None:
        if self._redis is None:
            return
        import json

        await self._redis.set(key, json.dumps(payload, ensure_ascii=False), ex=self.ttl_seconds)

    def _memory_resolve(self, name: str) -> str | None:
        normalized = _normalize_name(name)
        for key, code in self._mapping.items():
            if normalized == key or normalized in key or key in normalized:
                return code
        return None

    async def _fallback_llm(self, name: str) -> str | None:
        prompt = (
            "Return JSON only.\n"
            "Find closest Korean stock code in KR market for this company name.\n"
            f"Company: {name!s}\n"
            "Output: {\"code\":\"\", \"confidence\":0.0}"
        )
        response = await self._client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
        )
        text = response.choices[0].message.content if response.choices else "{}"
        try:
            payload = json.loads(str(text or "{}"))
            code = str(payload.get("code", "")).strip()
            if code:
                conf = _to_decimal(payload.get("confidence", "0"))
                return f"{code}:{conf}"
        except Exception:
            return None
        return None

    async def resolve(self, name: str, market: str = "KR") -> list[ResolvedCompany]:
        normalized = _normalize_name(name)
        cache_key = f"entity_resolver:{market}:{normalized}"
        cached = await self._cache_get(cache_key)
        if isinstance(cached, dict):
            cached_code = str(cached.get("code", "")).strip()
            cached_name = str(cached.get("name", name)).strip()
            cached_conf = _to_decimal(cached.get("confidence", "0"))
            if cached_code:
                return [ResolvedCompany(market=market, code=cached_code, name=cached_name, confidence=cached_conf)]

        if code := self._memory_resolve(name):
            result = [ResolvedCompany(market=market, code=code, name=name, confidence=Decimal("1"))]
            await self._cache_set(cache_key, {"code": code, "name": name, "confidence": "1"})
            return result

        llm = await self._fallback_llm(name)
        if not llm:
            return []
        if ":" in llm:
            code, confidence = llm.split(":", 1)
            confidence_value = _to_decimal(confidence)
        else:
            code = llm
            confidence_value = Decimal("0.5")
        result = [ResolvedCompany(market=market, code=code, name=name, confidence=confidence_value)]
        await self._cache_set(cache_key, {"code": code, "name": name, "confidence": str(confidence_value)})
        return result
