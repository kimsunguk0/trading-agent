from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

import core.execution.idempotency as idempotency_module
from core.execution.idempotency import OrderIdempotencyManager
from core.models.order import OrderRequest


def test_unknown_submitted_blocks_duplicate_for_same_account_symbol_side() -> None:
    manager = OrderIdempotencyManager()

    first = OrderRequest(
        account_id="acc",
        symbol="005930",
        side="BUY",
        quantity=1,
    )
    second = OrderRequest(
        account_id="acc",
        symbol="005930",
        side="BUY",
        quantity=2,
    )
    third = OrderRequest(
        account_id="acc",
        symbol="066570",
        side="BUY",
        quantity=1,
    )

    assert manager.can_submit(first)
    manager.reserve(first)
    manager.mark_unknown_submitted(first)

    assert not manager.can_submit(second)
    assert manager.can_submit(third)


def test_can_submit_after_finalization() -> None:
    manager = OrderIdempotencyManager()
    request = OrderRequest(
        account_id="acc",
        symbol="005930",
        side="SELL",
        quantity=1,
    )

    manager.reserve(request)
    manager.mark_unknown_submitted(request)
    manager.mark_finalized(request)

    assert manager.can_submit(request)


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self.rows: list[dict] = []

    async def execute(self, query: str, *args) -> None:
        self.executed.append((query, args))

    async def fetch(self, query: str, *args) -> list[dict]:
        self.executed.append((query, args))
        return self.rows

    async def close(self) -> None:
        return None


class _FakeAsyncpg:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    async def connect(self, dsn: str) -> _FakeConn:
        assert dsn == "postgresql://example"
        return self.conn


@pytest.mark.asyncio
async def test_idempotency_reserve_persists_submitted(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()
    monkeypatch.setattr(idempotency_module, "asyncpg", _FakeAsyncpg(conn))
    manager = OrderIdempotencyManager(dsn="postgresql://example", schema="trading_paper")
    request = OrderRequest(
        order_intent_id="OI-20260529-KR-005930-0001",
        account_id="acc",
        symbol="005930",
        side="BUY",
        quantity=Decimal("1"),
    )

    key = await manager.reserve_async(request)

    assert key
    assert conn.executed
    assert conn.executed[0][1][8] == "SUBMITTED"


@pytest.mark.asyncio
async def test_idempotency_load_blocks_active_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()
    conn.rows = [
        {
            "order_intent_id": "OI-20260529-KR-005930-0001",
            "idempotency_key": "idem-key",
            "account_id": "acc",
            "symbol": "005930",
            "side": "BUY",
            "status": "UNKNOWN_SUBMITTED",
            "created_at": datetime(2026, 5, 29, tzinfo=timezone.utc),
        }
    ]
    monkeypatch.setattr(idempotency_module, "asyncpg", _FakeAsyncpg(conn))
    manager = OrderIdempotencyManager(dsn="postgresql://example", schema="trading_paper")

    await manager.load()

    duplicate = OrderRequest(
        account_id="acc",
        symbol="005930",
        side="BUY",
        quantity=Decimal("1"),
    )
    assert not manager.can_submit(duplicate)
