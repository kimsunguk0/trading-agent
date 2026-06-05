from __future__ import annotations

from collections import defaultdict, deque
from decimal import Decimal

import pytest

from agents.decision.decision_engine import DecisionEngine
from core.events.schemas import EventType, SignalEvent
from core.models.market import Side, Symbol
from core.models.portfolio import Account


class _Bus:
    redis_url = None
    stream_prefix = "paper.events"
    _client = None

    def __init__(self) -> None:
        self.published: list[object] = []
        self._fallback_streams = defaultdict(deque)
        self._fallback_subscribers = defaultdict(list)

    async def publish(self, event: object) -> str:
        self.published.append(event)
        return "ok"

    def stream_name(self, event_type) -> str:
        value = event_type.value if hasattr(event_type, "value") else str(event_type)
        return f"{self.stream_prefix}.{value}"


class _PricingBroker:
    def __init__(self, *, orderbook: dict | None = None, quote: dict | None = None, fail_market_data: bool = False) -> None:
        self.orderbook = orderbook
        self.quote = quote
        self.fail_market_data = fail_market_data
        self.orderbook_calls: list[str] = []
        self.quote_calls: list[str] = []

    def get_account(self, account_id: str) -> Account:
        return Account(account_id=account_id, cash_balance=Decimal("1000000"))

    async def get_orderbook(self, symbol: str) -> dict:
        self.orderbook_calls.append(symbol)
        if self.fail_market_data:
            raise RuntimeError("market data down")
        if self.orderbook is None:
            raise RuntimeError("no orderbook")
        return self.orderbook

    async def get_quote(self, symbol: str) -> dict:
        self.quote_calls.append(symbol)
        if self.fail_market_data:
            raise RuntimeError("market data down")
        if self.quote is None:
            raise RuntimeError("no quote")
        return self.quote


def _signal(*, order_type: str, payload: dict | None = None) -> SignalEvent:
    data = {
        "market": "KR",
        "quantity": "1",
        "order_type": order_type,
    }
    if payload:
        data.update(payload)
    return SignalEvent(
        event_type=EventType.SIGNAL,
        strategy_id="pricing_test",
        account_id="default",
        symbol=Symbol("000660"),
        side=Side.BUY,
        signal_score=Decimal("1"),
        payload=data,
    )


@pytest.mark.asyncio
async def test_limit_signal_without_price_uses_best_ask_from_orderbook() -> None:
    broker = _PricingBroker(
        orderbook={
            "best_ask": {"price": Decimal("123500"), "quantity": 10},
            "best_bid": {"price": Decimal("123000"), "quantity": 20},
        }
    )
    engine = DecisionEngine(broker=broker, bus=_Bus(), account_id="default")

    event = await engine.process_signal(
        _signal(
            order_type="limit",
            payload={"limit_price_basis": "best_ask", "allow_market_order": False},
        )
    )

    assert event is not None
    assert event.request.order_type == "LIMIT"
    assert event.request.price == Decimal("123500")
    assert broker.orderbook_calls == ["000660"]


@pytest.mark.asyncio
async def test_limit_signal_without_market_data_and_market_fallback_disabled_is_skipped() -> None:
    broker = _PricingBroker(fail_market_data=True)
    bus = _Bus()
    engine = DecisionEngine(broker=broker, bus=bus, account_id="default")

    event = await engine.process_signal(
        _signal(
            order_type="limit",
            payload={"limit_price_basis": "best_ask", "allow_market_order": False},
        )
    )

    assert event is None
    assert not bus.published
    assert broker.orderbook_calls == ["000660"]
    assert broker.quote_calls == ["000660"]


@pytest.mark.asyncio
async def test_market_signal_passes_without_limit_price_or_market_data_lookup() -> None:
    broker = _PricingBroker(fail_market_data=True)
    engine = DecisionEngine(broker=broker, bus=_Bus(), account_id="default")

    event = await engine.process_signal(_signal(order_type="market"))

    assert event is not None
    assert event.request.order_type == "MARKET"
    assert event.request.price is None
    assert broker.orderbook_calls == []
    assert broker.quote_calls == []
