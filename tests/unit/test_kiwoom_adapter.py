from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from brokers.kiwoom_rest_kr_base import _parse_dec, _parse_num, _strip_code
from brokers.kiwoom_rest_kr_mock import KiwoomApiError, KiwoomRestKrMockAdapter
from core.models.order import OrderRequest


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _token_payload() -> dict:
    return {
        "token": "TOKEN-1",
        "token_type": "Bearer",
        "expires_dt": "20991231235959",
        "return_code": 0,
        "return_msg": "OK",
    }


def test_kiwoom_numeric_and_code_helpers_parse_measured_shapes() -> None:
    assert _parse_num("000000010000000") == 10000000
    assert _parse_num("-00000000003166") == -3166
    assert _parse_num("-00000000.89") == Decimal("-0.89")
    assert _parse_dec("000000100.00") == Decimal("100.00")
    assert _strip_code("A005930") == "005930"
    assert _strip_code("KR005930") == "005930"


@pytest.mark.asyncio
async def test_token_request_uses_kiwoom_secretkey_and_mock_host() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_token_payload())

    adapter = KiwoomRestKrMockAdapter(
        app_key="app",
        app_secret="sec",
        account_no="12345678",
        transport=httpx.MockTransport(handler),
    )

    await adapter._ensure_access_token()

    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert str(request.url) == "https://mockapi.kiwoom.com/oauth2/token"
    assert request.headers["Content-Type"] == "application/json;charset=UTF-8"
    assert json.loads(request.content) == {
        "grant_type": "client_credentials",
        "appkey": "app",
        "secretkey": "sec",
    }
    assert adapter._access_token == "TOKEN-1"


@pytest.mark.asyncio
async def test_token_refresh_triggers_within_10_minutes(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 5, 29, 12, 0, 0, tzinfo=timezone.utc)
    token_calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal token_calls
        token_calls += 1
        return httpx.Response(
            200,
            json={**_token_payload(), "token": "new-token"},
        )

    adapter = KiwoomRestKrMockAdapter(
        app_key="app",
        app_secret="sec",
        account_no="000",
        transport=httpx.MockTransport(handler),
    )
    monkeypatch.setattr(adapter, "_now", lambda: now)

    adapter._access_token = "old"
    adapter._access_token_expires_at = now + timedelta(minutes=11)
    await adapter._ensure_access_token()
    assert adapter._access_token == "old"
    assert token_calls == 0

    adapter._access_token_expires_at = now + timedelta(minutes=9)
    await adapter._ensure_access_token()
    assert adapter._access_token == "new-token"
    assert token_calls == 1


@pytest.mark.asyncio
async def test_rate_limiter_blocks_after_five_requests_per_second() -> None:
    from brokers.kiwoom_rest_kr_mock import _AsyncRateLimiter

    waits: list[float] = []
    now_values = iter([0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.0])

    async def fake_sleep(seconds: float) -> None:
        waits.append(seconds)

    limiter = _AsyncRateLimiter(
        max_requests_per_second=5,
        now_fn=lambda: next(now_values),
        sleep_fn=fake_sleep,
    )

    for _ in range(6):
        await limiter.acquire()
    assert waits == [1.0]


@pytest.mark.asyncio
async def test_return_code_non_zero_raises_kiwoom_api_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth2/token":
            return httpx.Response(200, json=_token_payload())
        return httpx.Response(200, json={"return_code": 100, "return_msg": "forced error"})

    adapter = KiwoomRestKrMockAdapter(
        app_key="app",
        app_secret="sec",
        account_no="000",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(KiwoomApiError) as exc:
        await adapter.get_quote("005930")

    assert exc.value.code == "100"


@pytest.mark.asyncio
async def test_return_code_non_zero_uses_return_msg_even_when_http_200() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth2/token":
            return httpx.Response(200, json=_token_payload())
        return httpx.Response(200, json=_fixture("kiwoom_error_missing_qry_tp.json"))

    adapter = KiwoomRestKrMockAdapter(
        app_key="app",
        app_secret="sec",
        account_no="000",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(KiwoomApiError) as exc:
        await adapter.get_cash("000")

    assert exc.value.code == "2"
    assert "qry_tp" in exc.value.message


@pytest.mark.asyncio
async def test_submit_buy_order_uses_kt10000_ordr_path_and_bearer_header() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/oauth2/token":
            return httpx.Response(200, json=_token_payload())
        return httpx.Response(200, json=_fixture("kiwoom_order_kt10000.json"))

    adapter = KiwoomRestKrMockAdapter(
        app_key="app",
        app_secret="sec",
        account_no="000",
        transport=httpx.MockTransport(handler),
    )

    request = OrderRequest(
        order_intent_id="OI-test",
        account_id="acc",
        symbol="005930",
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("100"),
        order_type="LIMIT",
    )
    ack = await adapter.submit_order(request)

    order_request = requests[-1]
    assert ack.order_id == "0081878"
    assert order_request.method == "POST"
    assert str(order_request.url) == "https://mockapi.kiwoom.com/api/dostk/ordr"
    assert order_request.headers["authorization"] == "Bearer TOKEN-1"
    assert order_request.headers["api-id"] == "kt10000"
    assert order_request.headers["cont-yn"] == "N"
    assert order_request.headers["next-key"] == ""
    assert json.loads(order_request.content) == {
        "dmst_stex_tp": "KRX",
        "stk_cd": "005930",
        "ord_qty": "1",
        "ord_uv": "100",
        "trde_tp": "0",
    }


@pytest.mark.asyncio
async def test_submit_sell_market_order_uses_kt10001_and_empty_price() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/oauth2/token":
            return httpx.Response(200, json=_token_payload())
        return httpx.Response(200, json={"return_code": 0, "return_msg": "OK", "ord_no": "ORDER-2"})

    adapter = KiwoomRestKrMockAdapter(
        app_key="app",
        app_secret="sec",
        account_no="000",
        transport=httpx.MockTransport(handler),
    )

    await adapter.submit_order(
        OrderRequest(
            order_intent_id="OI-sell",
            account_id="acc",
            symbol="005930",
            side="SELL",
            quantity=Decimal("2"),
            price=None,
            order_type="MARKET",
        )
    )

    order_request = requests[-1]
    assert order_request.headers["api-id"] == "kt10001"
    assert json.loads(order_request.content)["ord_uv"] == ""
    assert json.loads(order_request.content)["trde_tp"] == "3"


@pytest.mark.asyncio
async def test_cash_and_positions_use_kiwoom_account_api_ids() -> None:
    api_ids: list[str] = []
    bodies: list[dict] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/oauth2/token":
            return httpx.Response(200, json=_token_payload())
        api_ids.append(request.headers["api-id"])
        bodies.append(json.loads(request.content))
        if request.headers["api-id"] == "kt00001":
            return httpx.Response(200, json=_fixture("kiwoom_cash_kt00001.json"))
        return httpx.Response(200, json=_fixture("kiwoom_positions_kt00018.json"))

    adapter = KiwoomRestKrMockAdapter(
        app_key="app",
        app_secret="sec",
        account_no="000",
        transport=httpx.MockTransport(handler),
    )

    cash = await adapter.get_cash("000")
    positions = await adapter.get_positions("000")

    assert api_ids == ["kt00001", "kt00018"]
    assert bodies == [{"qry_tp": "1"}, {"qry_tp": "1", "dmst_stex_tp": "KRX"}]
    assert cash.cash_balance == Decimal("10000000")
    assert cash.available_cash == Decimal("7000000")
    assert positions["005930"]["name"] == "삼성전자"
    assert positions["005930"]["quantity"] == Decimal("30")
    assert positions["005930"]["average_price"] == Decimal("67997")
    assert positions["005930"]["current_price"] == Decimal("67890")
    assert positions["005930"]["unrealized_pnl"] == Decimal("-3166")
    assert positions["005930"]["profit_rate"] == Decimal("-0.89")
    assert adapter._last_position_summary == {
        "total_purchase_amount": Decimal("2039916"),
        "total_evaluation_amount": Decimal("2036750"),
        "total_evaluation_pnl": Decimal("-3166"),
        "total_profit_rate": Decimal("-0.16"),
        "estimated_deposit_asset_amount": Decimal("12036750"),
    }
