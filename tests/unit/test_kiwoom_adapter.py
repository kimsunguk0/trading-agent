from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from brokers.kiwoom_rest_kr_mock import KiwoomApiError, KiwoomRestKrMockAdapter
from core.models.order import OrderRequest


class _DummyResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


@pytest.mark.asyncio
async def test_token_refresh_triggers_within_10_minutes(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime(2026, 5, 29, 12, 0, 0, tzinfo=timezone.utc)
    adapter = KiwoomRestKrMockAdapter(app_key="app", app_secret="sec", account_no="000")
    monkeypatch.setattr(adapter, "_now", lambda: now)

    adapter._access_token = "old"
    adapter._access_token_expires_at = now + timedelta(minutes=11)

    called: list[tuple[str, str, bool, str | None]] = []

    async def fake_request(
        method: str,
        path: str,
        *,
        tr_id: str | None = None,
        params: dict | None = None,
        json_data: dict | None = None,
        skip_auth: bool = False,
    ) -> dict:
        called.append((method, path, skip_auth, tr_id))
        if path == "/oauth2/tokenP":
            return {"rt_cd": "0", "access_token": "new-token", "expires_in": "600"}
        return {"rt_cd": "0", "output": {}}

    monkeypatch.setattr(adapter, "_request", fake_request)

    await adapter._ensure_access_token()
    assert adapter._access_token == "old"
    assert len(called) == 0

    adapter._access_token_expires_at = now + timedelta(minutes=9)
    await adapter._ensure_access_token()
    assert adapter._access_token == "new-token"
    assert len(called) == 1


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
async def test_rt_cd_non_zero_raises_kis_api_error(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = KiwoomRestKrMockAdapter(app_key="app", app_secret="sec", account_no="000")
    adapter._access_token = "token"
    adapter._access_token_expires_at = datetime(2099, 1, 1, tzinfo=timezone.utc)

    async def fake_request(method: str, path: str, params=None, json=None, headers=None) -> _DummyResponse:
        return _DummyResponse({"rt_cd": "100", "msg1": "forced error"})

    monkeypatch.setattr(adapter._client, "request", fake_request)

    with pytest.raises(KiwoomApiError) as exc:
        await adapter._request("GET", "/uapi/domestic-stock/v1/trading/inquire-balance")

    assert exc.value.rt_cd == "100"


@pytest.mark.asyncio
async def test_submit_order_maps_buy_to_vttc0802u(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = KiwoomRestKrMockAdapter(app_key="app", app_secret="sec", account_no="000")
    captured: dict[str, str | None] = {}

    async def fake_request(
        method: str,
        path: str,
        *,
        tr_id: str | None = None,
        params: dict | None = None,
        json_data: dict | None = None,
        skip_auth: bool = False,
    ) -> dict:
        captured["tr_id"] = tr_id
        return {
            "rt_cd": "0",
            "output": {"ODNO": "ORDER-1"},
        }

    monkeypatch.setattr(adapter, "_request", fake_request)

    request = OrderRequest(
        order_intent_id="OI-test",
        account_id="acc",
        symbol="005930",
        side="BUY",
        quantity=Decimal("1"),
        price=Decimal("100"),
    )
    await adapter.submit_order(request)

    assert captured.get("tr_id") == "VTTC0802U"
