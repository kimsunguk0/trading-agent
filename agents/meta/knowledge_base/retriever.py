"""RAG retrieval for decision context."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx
from qdrant_client import QdrantClient

import os

from .indexer import _to_decimal, _to_str


class JournalRetriever:
    def __init__(
        self,
        *,
        qdrant_url: str | None = None,
        qdrant_collection: str = "journal_entries",
        embedding_api_url: str | None = None,
        qdrant_client_factory=None,
    ) -> None:
        self.qdrant_collection = qdrant_collection
        self.embedding_api_url = embedding_api_url or os.getenv("EMBEDDING_API_URL", "http://localhost:8001")
        self._http = httpx.Client(timeout=10.0)
        if qdrant_client_factory is not None:
            self._qdrant = qdrant_client_factory()
        else:
            self._qdrant = QdrantClient(url=qdrant_url or os.getenv("QDRANT_URL", "http://localhost:6333"))

    def close(self) -> None:
        self._http.close()

    def _embed(self, text: str) -> list[float]:
        response = self._http.post(
            f"{self.embedding_api_url}/v1/embeddings",
            json={"model": "bge-m3", "input": text},
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            first = payload["data"][0]
            embedding = first.get("embedding") if isinstance(first, dict) else None
            if isinstance(embedding, list):
                return [float(x) for x in embedding]
        if isinstance(payload, dict) and isinstance(payload.get("embedding"), list):
            return [float(x) for x in payload["embedding"]]
        return []

    def search(
        self,
        *,
        symbol: str,
        news_summary: str,
        regime: str,
        top_k: int = 3,
    ) -> list[dict[str, Any]]:
        try:
            vector = self._embed(f"{symbol} | {regime} | {news_summary}")
            if not vector:
                return []

            hits = self._qdrant.search(
                collection_name=self.qdrant_collection,
                query_vector=vector,
                limit=int(top_k),
                with_payload=True,
            )
        except Exception:
            return []

        if not hits:
            return []

        results: list[dict[str, Any]] = []
        for hit in hits:
            payload = hit.payload or {}
            if not isinstance(payload, dict):
                payload = {}
            results.append(
                {
                    "symbol": _to_str(payload.get("symbol_code", symbol)),
                    "strategy_id": _to_str(payload.get("strategy_id")),
                    "narrative": _to_str(payload.get("narrative")),
                    "lessons": _to_str(payload.get("lessons")),
                    "pnl_pct": _to_decimal(payload.get("pnl_pct", "0")),
                    "score": _to_decimal(hit.score) if hasattr(hit, "score") else Decimal("0"),
                    "journal_id": _to_str(payload.get("journal_id")),
                    "created_at": _to_str(payload.get("created_at", str(datetime.now(timezone.utc)))),
                }
            )
        return results
