from __future__ import annotations

import asyncio
from decimal import Decimal
from datetime import datetime, timezone
from types import SimpleNamespace

from agents.meta.knowledge_base.indexer import JournalIndexer
from agents.meta.knowledge_base.retriever import JournalRetriever


def _fake_embedding(*_args, **_kwargs):
    return [0.1] * 1024


class FakePoint(SimpleNamespace):
    pass


class FakeQdrant:
    def __init__(self) -> None:
        self.created = False
        self.points: list[FakePoint] = []

    def get_collection(self, *_args, **_kwargs):
        if not self.created:
            raise RuntimeError("not found")
        return True

    def recreate_collection(self, *_args, **_kwargs) -> None:
        self.created = True

    def upsert(self, collection_name: str, points: list[FakePoint]) -> None:
        self.points.extend(points)

    def search(self, collection_name: str, query_vector: list[float], limit: int, with_payload: bool = False):
        assert collection_name == "journal_entries"
        return [
            SimpleNamespace(
                id=p.id,
                payload=p.payload,
                score=1.0 / (idx + 1),
            )
            for idx, p in enumerate(self.points[:limit])
        ]


def test_journal_entry_indexing_search_round_trip() -> None:
    fake = FakeQdrant()

    async def fake_embed_async(_: str) -> list[float]:
        return _fake_embedding()

    indexer = JournalIndexer(
        dsn=None,
        qdrant_client_factory=lambda: fake,
    )
    indexer._embed = fake_embed_async  # type: ignore[method-assign]

    inserted = asyncio.run(
        indexer.index_entries(
            [
                {
                    "id": "1",
                    "symbol_code": "AAPL",
                    "strategy_id": "us_earnings_reaction_v1",
                    "pnl_pct": "0.0325",
                    "narrative": "Strong revenue beat on guidance shift.",
                    "lessons": "Trim risk after upside run.",
                    "created_at": datetime(2026, 5, 29, 0, 0, tzinfo=timezone.utc),
                }
            ]
        )
    )
    assert inserted == 1

    retriever = JournalRetriever(qdrant_client_factory=lambda: fake)

    def fake_embed(_: str) -> list[float]:
        return _fake_embedding()

    retriever._embed = fake_embed  # type: ignore[method-assign]
    found = retriever.search(symbol="AAPL", news_summary="earnings beat", regime="bull_trend", top_k=3)

    assert len(found) == 1
    assert found[0]["symbol"] == "AAPL"
    assert found[0]["narrative"] == "Strong revenue beat on guidance shift."
    assert found[0]["lessons"] == "Trim risk after upside run."
    assert found[0]["pnl_pct"] == Decimal("0.0325")


def test_similarity_search_returns_top_3() -> None:
    fake = FakeQdrant()
    def fake_embed(_: str) -> list[float]:
        return _fake_embedding()

    fake.points = [
        SimpleNamespace(
            id=str(i),
            payload={
                "symbol_code": f"SYM{i}",
                "strategy_id": "us_earnings_reaction_v1",
                "pnl_pct": str(i * 0.01),
                "narrative": f"narrative{i}",
                "lessons": f"lessons{i}",
                "created_at": datetime(2026, 5, 29, 0, 0, tzinfo=timezone.utc).isoformat(),
            },
            score=1.0 / (i + 1),
        )
        for i in range(5)
    ]
    fake.created = True

    retriever = JournalRetriever(qdrant_client_factory=lambda: fake)
    retriever._embed = fake_embed  # type: ignore[method-assign]
    results = retriever.search(symbol="AAA", news_summary="earnings", regime="bull_trend", top_k=3)

    assert len(results) == 3
    assert results[0]["symbol"] in {"SYM0", "SYM1", "SYM2", "SYM3", "SYM4"}


def test_graceful_empty_collection_returns_empty() -> None:
    fake = FakeQdrant()
    fake.created = True

    retriever = JournalRetriever(qdrant_client_factory=lambda: fake)
    def fake_embed(_: str) -> list[float]:
        return _fake_embedding()
    retriever._embed = fake_embed  # type: ignore[method-assign]
    results = retriever.search(symbol="AAPL", news_summary="", regime="", top_k=3)

    assert results == []
