"""Broker protocol and capability model."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from decimal import Decimal

from core.models.order import OrderAck, OrderRequest


@dataclass
class BrokerCapabilities:
    name: str
    broker: str = "simulated"
    market: str = "KR"
    environment: str = "paper"
    supports_market_order: bool = True
    supports_limit_order: bool = True
    supports_partial_fill: bool = True
    can_query_order_status: bool = True
    supports_cancel_order: bool = True
    supports_client_order_id: bool = False
    supports_fractional_quantity: bool = False
    min_order_quantity: Decimal = Decimal("1")
    supports_stop_order: bool = False
    supports_extended_hours: bool = False
    supported_time_in_force: frozenset[str] = frozenset({"DAY", "IOC", "FOK"})
    max_requests_per_second: int = 5
    market_open_utc: tuple[int, int] = (0, 0)
    market_close_utc: tuple[int, int] = (0, 0)
    simulated: bool = False


class BrokerAdapter(Protocol):
    capabilities: BrokerCapabilities

    async def submit_order(self, request: OrderRequest) -> OrderAck | None:
        ...

    async def get_order_status(self, order_intent_id: str) -> OrderAck | None:
        ...

    async def get_cash_snapshot(self, account_id: str):
        ...
