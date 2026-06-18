"""Broker capability descriptions used by adapters and runtime."""

from __future__ import annotations

from decimal import Decimal

from brokers.base import BrokerCapabilities


def _to_hhmm(value: int) -> tuple[int, int]:
    return value // 100, value % 100


SIMULATED_CAPS = BrokerCapabilities(
    name="simulated",
    broker="simulated",
    market="KR",
    environment="paper",
    supports_market_order=True,
    supports_limit_order=True,
    supports_partial_fill=True,
    can_query_order_status=True,
    supports_cancel_order=True,
    supports_client_order_id=False,
    supports_fractional_quantity=False,
    min_order_quantity=Decimal("1"),
    supports_stop_order=False,
    supported_time_in_force=frozenset({"DAY", "IOC", "FOK"}),
    max_requests_per_second=5,
    market_open_utc=_to_hhmm(0),
    market_close_utc=_to_hhmm(630),
    supports_extended_hours=False,
    simulated=True,
)

KIWOOM_KR_MOCK_CAPS = BrokerCapabilities(
    name="kiwoom_kr_mock",
    broker="kiwoom",
    market="KR",
    environment="paper",
    supports_market_order=True,
    supports_limit_order=True,
    supports_partial_fill=True,
    can_query_order_status=True,
    supports_cancel_order=True,
    supports_client_order_id=False,
    supports_fractional_quantity=False,
    min_order_quantity=Decimal("1"),
    supports_stop_order=False,
    supported_time_in_force=frozenset({"DAY", "IOC", "FOK"}),
    max_requests_per_second=5,
    market_open_utc=_to_hhmm(0),
    market_close_utc=_to_hhmm(630),
    supports_extended_hours=False,
    simulated=True,
)

KIWOOM_KR_LIVE_CAPS = BrokerCapabilities(
    name="kiwoom_kr_live",
    broker="kiwoom",
    market="KR",
    environment="live",
    supports_market_order=True,
    supports_limit_order=True,
    supports_partial_fill=True,
    can_query_order_status=True,
    supports_cancel_order=True,
    supports_client_order_id=False,
    supports_fractional_quantity=False,
    min_order_quantity=Decimal("1"),
    supports_stop_order=False,
    supported_time_in_force=frozenset({"DAY", "IOC", "FOK"}),
    max_requests_per_second=5,
    market_open_utc=_to_hhmm(0),
    market_close_utc=_to_hhmm(630),
    supports_extended_hours=False,
)

KIS_KR_MOCK_CAPS = BrokerCapabilities(
    name="kis_domestic_kr_mock",
    broker="kis",
    market="KR",
    environment="paper",
    supports_market_order=True,
    supports_limit_order=True,
    supports_partial_fill=True,
    can_query_order_status=True,
    supports_cancel_order=True,
    supports_client_order_id=False,
    supports_fractional_quantity=False,
    min_order_quantity=Decimal("1"),
    supports_stop_order=False,
    supported_time_in_force=frozenset({"DAY", "IOC", "FOK"}),
    max_requests_per_second=5,
    market_open_utc=_to_hhmm(0),
    market_close_utc=_to_hhmm(630),
    supports_extended_hours=False,
    simulated=True,
)

KIS_KR_LIVE_CAPS = BrokerCapabilities(
    name="kis_domestic_kr_live",
    broker="kis",
    market="KR",
    environment="live",
    supports_market_order=True,
    supports_limit_order=True,
    supports_partial_fill=True,
    can_query_order_status=True,
    supports_cancel_order=True,
    supports_client_order_id=False,
    supports_fractional_quantity=False,
    min_order_quantity=Decimal("1"),
    supports_stop_order=False,
    supported_time_in_force=frozenset({"DAY", "IOC", "FOK"}),
    max_requests_per_second=5,
    market_open_utc=_to_hhmm(0),
    market_close_utc=_to_hhmm(630),
    supports_extended_hours=False,
)

KIS_US_MOCK_CAPS = BrokerCapabilities(
    name="kis_overseas_mock",
    broker="kis",
    market="US",
    environment="paper",
    supports_market_order=True,
    supports_limit_order=True,
    supports_partial_fill=True,
    can_query_order_status=True,
    supports_cancel_order=True,
    supports_client_order_id=False,
    supports_fractional_quantity=True,
    min_order_quantity=Decimal("0.0001"),
    supports_stop_order=True,
    supported_time_in_force=frozenset({"DAY", "IOC"}),
    max_requests_per_second=20,
    market_open_utc=_to_hhmm(1430),
    market_close_utc=_to_hhmm(2100),
    supports_extended_hours=True,
)

KIS_US_LIVE_CAPS = BrokerCapabilities(
    name="kis_overseas_live",
    broker="kis",
    market="US",
    environment="live",
    supports_market_order=True,
    supports_limit_order=True,
    supports_partial_fill=True,
    can_query_order_status=True,
    supports_cancel_order=True,
    supports_client_order_id=False,
    supports_fractional_quantity=True,
    min_order_quantity=Decimal("0.0001"),
    supports_stop_order=True,
    supported_time_in_force=frozenset({"DAY", "IOC"}),
    max_requests_per_second=20,
    market_open_utc=_to_hhmm(1430),
    market_close_utc=_to_hhmm(2100),
    supports_extended_hours=True,
)

TOSS_CAPS = BrokerCapabilities(
    name="toss_invest",
    broker="toss",
    market="KR,US",
    environment="live",
    supports_market_order=True,
    supports_limit_order=True,
    supports_partial_fill=True,
    can_query_order_status=True,
    supports_cancel_order=True,
    supports_client_order_id=True,
    supports_fractional_quantity=False,
    min_order_quantity=Decimal("1"),
    supports_stop_order=False,
    supported_time_in_force=frozenset({"DAY", "CLS"}),
    max_requests_per_second=5,
    market_open_utc=_to_hhmm(0),
    market_close_utc=_to_hhmm(630),
    supports_extended_hours=False,
    simulated=False,
)
