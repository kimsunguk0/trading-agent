"""TossInvestAdapter MVP-6 placeholder.

Toss 투자 REST/FCM endpoint mapping is not yet publicly available in this
repository context (MVP 6 API placeholder).
"""

from __future__ import annotations

from decimal import Decimal

from brokers.base import BrokerAdapter, BrokerCapabilities
from core.models.order import OrderAck, OrderRequest


# 기대되는 Toss Invest API 엔드포인트(예정):
# - POST /api/v1/auth/token            : OAuth token 발급
# - GET  /api/v1/accounts/{account_id} : 계좌/잔고 조회
# - GET  /api/v1/markets/{symbol}      : 실시간/지연 시세 조회
# - GET  /api/v1/orderbook/{symbol}    : 호가/유동성 조회
# - POST /api/v1/orders               : 주문 제출
# - GET  /api/v1/orders/{order_id}     : 주문 상태 조회
# - DELETE /api/v1/orders/{order_id}   : 주문 취소


TOSS_CAPS = BrokerCapabilities(
    name="toss_invest_future",
    broker="toss",
    market="KR",
    environment="future",
    supports_market_order=None,
    supports_limit_order=None,
    supports_partial_fill=None,
    can_query_order_status=None,
    supports_cancel_order=None,
    supports_client_order_id=None,
    supports_fractional_quantity=None,
    min_order_quantity=Decimal("1"),
    supports_stop_order=None,
    supports_extended_hours=None,
    supported_time_in_force=frozenset(),
    max_requests_per_second=0,
    market_open_utc=(900, 0),
    market_close_utc=(1530, 0),
    simulated=False,
)


class TossInvestAdapter:
    """Placeholder adapter implementing BrokerAdapter for MVP 6."""

    name: str = "toss_invest_future"
    capabilities = TOSS_CAPS

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
        account_id: str | None = None,
        base_url: str | None = None,
    ) -> None:
        self.api_key = api_key or ""
        self.api_secret = api_secret or ""
        self.account_id = account_id or ""
        self.base_url = base_url or "https://api.tossinvest.com"

    async def submit_order(self, request: OrderRequest) -> OrderAck | None:
        raise NotImplementedError("TossInvestAdapter: API not yet released")

    async def get_order_status(self, order_intent_id: str) -> OrderAck | None:
        raise NotImplementedError("TossInvestAdapter: API not yet released")

    async def get_cash_snapshot(self, account_id: str):
        raise NotImplementedError("TossInvestAdapter: API not yet released")
