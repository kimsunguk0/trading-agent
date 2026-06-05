"""Typed in-memory event models."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

from core.models.market import Market, Side, Symbol
from core.models.order import Fill, OrderRequest


class EventType(str, Enum):
    NEWS = "news"
    MARKET_TICK = "market_tick"
    SIGNAL = "signal"
    ORDER_INTENT = "order_intent"
    FILL = "fill"
    RISK = "risk"
    CORPORATE_ACTION = "corporate_action"


# Canonical stream suffix for executable order intents.
# The event_type remains singular "order_intent", but execution workers consume
# the plural Redis stream suffix: {REDIS_STREAM_PREFIX}.order_intents.
ORDER_INTENTS_STREAM = "order_intents"


class Event(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict[str, Any] = Field(default_factory=dict)


class NewsEvent(Event):
    title: str
    body: str
    source: str
    symbol: Symbol | None = None


class MarketTickEvent(Event):
    symbol: Symbol
    market: Market
    bid: Decimal
    ask: Decimal
    price: Decimal
    volume: Decimal


class CorporateActionEvent(Event):
    market: str
    symbol: str
    action_type: str
    title: str | None = None
    cash_amount: Decimal | None = None
    shares_per_stock: Decimal | None = None
    ratio: Decimal | None = None
    as_of: datetime


class SignalEvent(Event):
    strategy_id: str
    account_id: str
    symbol: Symbol
    side: Side
    signal_score: Decimal


class OrderIntentEvent(Event):
    request: OrderRequest


class FillEvent(Event):
    fill_id: str
    fill: Fill


class RiskEvent(Event):
    order_intent_id: str
    stage: str
    passed: bool
    reason: str | None = None
