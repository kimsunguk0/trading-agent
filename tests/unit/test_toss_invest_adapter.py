from __future__ import annotations

import base64
import json
from decimal import Decimal

import httpx
import pytest

from brokers.toss_invest_future import TossInvestAdapter
from core.models.order import OrderRequest, OrderStatus


def _token_payload() -> dict:
    return {"access_token": "TOSS-TOKEN", "expires_in": 3600, "token_type": "Bearer"}


def _basic(app_key: str = "app", app_secret: str = "sec") -> str:
    return "Basic " + base64.b64encode(f"{app_key}:{app_secret}".encode("utf-8")).decode("ascii")


@pytest.mark.asyncio
async def test_toss_token_request_uses_basic_auth_and_form_body() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_token_payload())

    adapter = TossInvestAdapter(
        app_key="app",
        app_secret="sec",
        account_no="ACC-1",
        transport=httpx.MockTransport(handler),
    )

    await adapter._ensure_access_token()

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert str(request.url) == "https://openapi.tossinvest.com/oauth2/token"
    assert request.headers["Authorization"] == _basic()
    assert request.headers["Content-Type"] == "application/x-www-form-urlencoded"
    assert request.content == b"grant_type=client_credentials"
    assert adapter._access_token == "TOSS-TOKEN"
    await adapter.close()


@pytest.mark.asyncio
async def test_toss_get_quote_sends_bearer_header_and_parses_defensive_fields() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/oauth2/token":
            return httpx.Response(200, json=_token_payload())
        assert request.url.path == "/api/v1/prices"
        assert request.url.params["symbol"] == "005930"
        assert request.headers["Authorization"] == "Bearer TOSS-TOKEN"
        assert "X-Tossinvest-Account" not in request.headers
        return httpx.Response(
            200,
            json=[
                {
                    "symbol": "005930",
                    "timestamp": "2026-06-18T09:00:00+09:00",
                    "lastPrice": "71,500",
                    "currency": "KRW",
                }
            ],
        )

    adapter = TossInvestAdapter(
        app_key="app",
        app_secret="sec",
        account_no="ACC-1",
        transport=httpx.MockTransport(handler),
    )

    quote = await adapter.get_quote("005930")

    assert quote["symbol"] == "005930"
    assert quote["currency"] == "KRW"
    assert quote["price"] == Decimal("71500")
    assert quote["occurred_at"] == "2026-06-18T09:00:00+09:00"
    await adapter.close()


@pytest.mark.asyncio
async def test_toss_get_cash_is_not_implemented_without_official_endpoint() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("get_cash must not call a guessed Toss endpoint")

    adapter = TossInvestAdapter(
        app_key="app",
        app_secret="sec",
        account_no="ACC-1",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(NotImplementedError, match="cash balance endpoint"):
        await adapter.get_cash("ACC-1")
    await adapter.close()


@pytest.mark.asyncio
async def test_toss_get_positions_uses_holdings_endpoint_and_parses_items() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/oauth2/token":
            return httpx.Response(200, json=_token_payload())
        assert request.headers["Authorization"] == "Bearer TOSS-TOKEN"
        assert request.headers["X-Tossinvest-Account"] == "ACC-1"
        if request.url.path == "/api/v1/holdings":
            return httpx.Response(
                200,
                json={
                    "totalPurchaseAmount": {"krw": "140000", "usd": "0"},
                    "marketValue": {"amount": {"krw": "143000", "usd": "0"}},
                    "profitLoss": {"amount": {"krw": "3000", "usd": "0"}, "rate": "2.14"},
                    "items": [
                        {
                            "symbol": "005930",
                            "name": "삼성전자",
                            "quantity": "2",
                            "lastPrice": "71500",
                            "averagePurchasePrice": "70000",
                            "marketValue": {
                                "purchaseAmount": "140000",
                                "amount": "143000",
                                "amountAfterCost": "142900",
                            },
                            "profitLoss": {
                                "amount": "3000",
                                "amountAfterCost": "2900",
                                "rate": "2.14",
                                "rateAfterCost": "2.07",
                            },
                        }
                    ]
                },
            )
        raise AssertionError(f"unexpected path {request.url.path}")

    adapter = TossInvestAdapter(
        app_key="app",
        app_secret="sec",
        account_no="ACC-1",
        transport=httpx.MockTransport(handler),
    )

    positions = await adapter.get_positions("ACC-1")

    assert positions["005930"]["quantity"] == Decimal("2")
    assert positions["005930"]["average_price"] == Decimal("70000")
    assert positions["005930"]["current_price"] == Decimal("71500")
    assert positions["005930"]["unrealized_pnl"] == Decimal("3000")
    assert positions["005930"]["profit_rate"] == Decimal("2.14")
    assert positions["005930"]["evaluation_amount"] == Decimal("143000")
    assert positions["005930"]["purchase_amount"] == Decimal("140000")
    assert {request.url.path for request in requests} >= {
        "/oauth2/token",
        "/api/v1/holdings",
    }
    await adapter.close()


@pytest.mark.asyncio
async def test_toss_orderbook_defensive_parsing() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth2/token":
            return httpx.Response(200, json=_token_payload())
        assert request.url.path == "/api/v1/orderbook"
        assert request.url.params["symbol"] == "005930"
        return httpx.Response(
            200,
            json={
                "timestamp": "2026-06-18T09:00:01+09:00",
                "currency": "KRW",
                "asks": [{"price": "71600", "volume": "10"}, {"price": "71550", "volume": "5"}],
                "bids": [{"price": "71500", "volume": "7"}, {"price": "71400", "volume": "9"}],
            },
        )

    adapter = TossInvestAdapter(
        app_key="app",
        app_secret="sec",
        account_no="ACC-1",
        transport=httpx.MockTransport(handler),
    )

    orderbook = await adapter.get_orderbook("005930")

    assert orderbook["best_ask"] == {"price": Decimal("71550"), "quantity": 5}
    assert orderbook["best_bid"] == {"price": Decimal("71500"), "quantity": 7}
    await adapter.close()


@pytest.mark.asyncio
async def test_toss_submit_order_posts_account_header_and_order_json() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/oauth2/token":
            return httpx.Response(200, json=_token_payload())
        assert request.method == "POST"
        assert request.url.path == "/api/v1/orders"
        assert request.headers["Authorization"] == "Bearer TOSS-TOKEN"
        assert request.headers["X-Tossinvest-Account"] == "ACC-1"
        assert request.headers["Content-Type"] == "application/json"
        assert json.loads(request.content) == {
            "symbol": "005930",
            "side": "BUY",
            "quantity": "1",
            "orderType": "LIMIT",
            "clientOrderId": "OI-1",
            "price": "71500",
        }
        return httpx.Response(
            200,
            json={"orderId": "TOSS-ORDER-1", "clientOrderId": "OI-1"},
        )

    adapter = TossInvestAdapter(
        app_key="app",
        app_secret="sec",
        account_no="ACC-1",
        transport=httpx.MockTransport(handler),
    )

    ack = await adapter.submit_order(
        OrderRequest(
            order_intent_id="OI-1",
            account_id="ACC-1",
            symbol="005930",
            side="BUY",
            quantity=Decimal("1"),
            price=Decimal("71500"),
            order_type="LIMIT",
        )
    )

    assert ack.order_id == "TOSS-ORDER-1"
    assert ack.order_intent_id == "OI-1"
    assert ack.status == OrderStatus.SUBMITTED
    assert len([request for request in requests if request.url.path == "/api/v1/orders"]) == 1
    assert adapter.capabilities.supports_client_order_id is True
    await adapter.close()
