from __future__ import annotations

import asyncio
import random
from decimal import Decimal

import pytest

from brokers.simulated import SimulatedBrokerAdapter
from core.models.order import OrderRequest, OrderStatus


@pytest.mark.asyncio
async def test_simulated_partial_fill(monkeypatch) -> None:
    adapter = SimulatedBrokerAdapter()
    # no delay/no timeout / partial fill
    seq = iter([0.5, 0.99, 0.05, 0.6])

    def fake_random() -> float:
        return next(seq)

    monkeypatch.setattr(random, "random", fake_random)

    request = OrderRequest(
        account_id="acc",
        symbol="005930",
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("100"),
    )
    ack = await adapter.submit_order(request)
    assert ack is not None
    assert ack.filled_quantity > Decimal("0")
    assert ack.filled_quantity < request.quantity


@pytest.mark.asyncio
async def test_simulated_unknown_timeout(monkeypatch) -> None:
    adapter = SimulatedBrokerAdapter()
    seq = iter([0.5, 0.01])

    def fake_random() -> float:
        return next(seq)

    monkeypatch.setattr(random, "random", fake_random)

    request = OrderRequest(
        account_id="acc",
        symbol="005930",
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("100"),
    )
    with pytest.raises(asyncio.TimeoutError):
        await adapter.submit_order(request)


@pytest.mark.asyncio
async def test_simulated_rejection_insufficient_cash() -> None:
    adapter = SimulatedBrokerAdapter(initial_cash=Decimal("10"))
    adapter.delay_probability = Decimal("0")
    adapter.timeout_probability = Decimal("0")
    request = OrderRequest(
        account_id="acc",
        symbol="005930",
        side="BUY",
        quantity=Decimal("10"),
        price=Decimal("100"),
    )
    ack = await adapter.submit_order(request)
    assert ack is not None
    assert ack.status == OrderStatus.REJECTED
