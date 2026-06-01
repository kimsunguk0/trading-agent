"""Journal vector indexer for RAG."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import asyncpg
import httpx
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, PointStruct, VectorParams

import os


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.now(timezone.utc)


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


@dataclass
class JournalEntry:
    id: str
    symbol: str
    strategy_id: str
    pnl_pct: Decimal
    narrative: str
    lessons: str
    created_at: datetime


class JournalIndexer:
    def __init__(
        self,
        *,
        dsn: str | None = None,
        schema: str | None = None,
        qdrant_url: str | None = None,
        qdrant_collection: str = "journal_entries",
        embedding_api_url: str | None = None,
        vector_size: int = 1024,
        qdrant_client_factory: Callable[[], QdrantClient] | None = None,
    ) -> None:
        self.dsn = dsn or os.getenv("DATABASE_URL")
        self.schema = schema or f"trading_{os.getenv('ENVIRONMENT', 'paper')}"
        self.qdrant_collection = qdrant_collection
        self.embedding_api_url = embedding_api_url or os.getenv("EMBEDDING_API_URL", "http://localhost:8001")
        self.vector_size = vector_size
        self._last_seen_at = datetime.now(timezone.utc) - timedelta(days=7)

        if qdrant_client_factory is not None:
            self._qdrant = qdrant_client_factory()
        else:
            self._qdrant = QdrantClient(url=qdrant_url or os.getenv("QDRANT_URL", "http://localhost:6333"))

        self._http = httpx.AsyncClient(timeout=10.0)
        self._collection_ready = False

    async def _ensure_collection(self) -> None:
        if self._collection_ready:
            return

        try:
            self._qdrant.get_collection(self.qdrant_collection)
            self._collection_ready = True
            return
        except Exception:
            pass

        self._qdrant.recreate_collection(
            collection_name=self.qdrant_collection,
            vectors_config=VectorParams(size=self.vector_size, distance=Distance.COSINE),
        )
        self._collection_ready = True

    async def close(self) -> None:
        await self._http.aclose()

    def _document_text(self, row: JournalEntry) -> str:
        return f"{row.narrative} {row.lessons}".strip()

    async def _embed(self, text: str) -> list[float]:
        response = await self._http.post(
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

    async def _query_rows(
        self,
        *,
        limit: int | None = None,
        since: datetime | None = None,
    ) -> list[JournalEntry]:
        if not self.dsn:
            return []

        query_limit = max(1, int(limit or 500))
        rows: list[dict[str, Any]] = []
        conn = await asyncpg.connect(self.dsn)
        try:
            if since is None:
                sql = f"""
                    SELECT id, symbol_code, strategy_id, COALESCE(pnl_pct, 0) AS pnl_pct,
                           COALESCE(narrative, '') AS narrative,
                           COALESCE(lessons, '') AS lessons,
                           COALESCE(created_at, NOW()) AS created_at
                    FROM {self.schema}.journal_entries
                    ORDER BY created_at DESC
                    LIMIT $1
                """
                rows = [dict(row) for row in await conn.fetch(sql, query_limit)]
            else:
                sql = f"""
                    SELECT id, symbol_code, strategy_id, COALESCE(pnl_pct, 0) AS pnl_pct,
                           COALESCE(narrative, '') AS narrative,
                           COALESCE(lessons, '') AS lessons,
                           COALESCE(created_at, NOW()) AS created_at
                    FROM {self.schema}.journal_entries
                    WHERE created_at > $1
                    ORDER BY created_at DESC
                    LIMIT $2
                """
                rows = [dict(row) for row in await conn.fetch(sql, since, query_limit)]
        finally:
            await conn.close()

        result: list[JournalEntry] = []
        for row in rows:
            result.append(
                JournalEntry(
                    id=_to_str(row.get("id")),
                    symbol=_to_str(row.get("symbol_code")),
                    strategy_id=_to_str(row.get("strategy_id")),
                    pnl_pct=_to_decimal(row.get("pnl_pct")),
                    narrative=_to_str(row.get("narrative")),
                    lessons=_to_str(row.get("lessons")),
                    created_at=_to_datetime(row.get("created_at")),
                )
            )
        return result

    async def _point_payload(self, row: JournalEntry) -> dict[str, Any]:
        return {
            "journal_id": row.id,
            "symbol_code": row.symbol,
            "strategy_id": row.strategy_id,
            "pnl_pct": str(row.pnl_pct),
            "narrative": row.narrative,
            "lessons": row.lessons,
            "created_at": row.created_at.isoformat(),
        }

    async def index_recent_entries(self, *, limit: int = 200) -> int:
        await self._ensure_collection()
        rows = await self._query_rows(limit=limit)
        if not rows:
            return 0

        points: list[PointStruct] = []
        for row in rows:
            vector = await self._embed(self._document_text(row))
            if not vector:
                continue
            if len(vector) != self.vector_size:
                continue
            payload = await self._point_payload(row)
            points.append(PointStruct(id=row.id, vector=vector, payload=payload))
            self._last_seen_at = max(self._last_seen_at, row.created_at)

        if points:
            self._qdrant.upsert(collection_name=self.qdrant_collection, points=points)
        return len(points)

    async def index_new_entries(self, *, limit: int = 200) -> int:
        await self._ensure_collection()
        rows = await self._query_rows(limit=limit, since=self._last_seen_at)
        if not rows:
            return 0

        points: list[PointStruct] = []
        max_created = self._last_seen_at
        for row in rows:
            vector = await self._embed(self._document_text(row))
            if not vector or len(vector) != self.vector_size:
                continue
            payload = await self._point_payload(row)
            points.append(PointStruct(id=row.id, vector=vector, payload=payload))
            if row.created_at > max_created:
                max_created = row.created_at

        if points:
            self._qdrant.upsert(collection_name=self.qdrant_collection, points=points)
            self._last_seen_at = max_created
        return len(points)

    async def auto_index_loop(self, poll_interval_seconds: int = 60, *, limit: int = 200) -> None:
        while True:
            await self.index_new_entries(limit=limit)
            await asyncio.sleep(max(1, int(poll_interval_seconds)))

    async def index_entries(self, entries: list[dict[str, Any]]) -> int:
        await self._ensure_collection()
        if not entries:
            return 0

        points: list[PointStruct] = []
        for row in entries:
            item = JournalEntry(
                id=_to_str(row.get("id")),
                symbol=_to_str(row.get("symbol_code", row.get("symbol", ""))),
                strategy_id=_to_str(row.get("strategy_id", "")),
                pnl_pct=_to_decimal(row.get("pnl_pct")),
                narrative=_to_str(row.get("narrative", "")),
                lessons=_to_str(row.get("lessons", "")),
                created_at=row.get("created_at") if isinstance(row.get("created_at"), datetime) else datetime.now(timezone.utc),
            )
            vector = await self._embed(self._document_text(item))
            if not vector or len(vector) != self.vector_size:
                continue
            points.append(PointStruct(id=item.id, vector=vector, payload=await self._point_payload(item)))

        if points:
            self._qdrant.upsert(collection_name=self.qdrant_collection, points=points)
            return len(points)
        return 0
