from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
import yaml

from brokers.kis_overseas_mock import KISOverseasMockAdapter
from core.clock import is_market_open
from core.models.order import OrderRequest


@pytest.mark.asyncio
async def test_kis_overseas_submit_order_preserves_fractional_quantity(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = KISOverseasMockAdapter(app_key="app", app_secret="secret", account_no="000")

    captured: dict[str, object] = {}

    async def fake_request(
        method: str,
        path: str,
        *,
        tr_id: str | None = None,
        params: dict | None = None,
        json_data: dict | None = None,
        skip_auth: bool = False,
    ) -> dict:
        if path == "/oauth2/tokenP":
            return {"rt_cd": "0", "access_token": "tok", "expires_in": "86400"}
        captured["path"] = path
        captured["payload"] = json_data or {}
        return {"rt_cd": "0", "output": {"ODNO": "ORDER-1"}}

    monkeypatch.setattr(adapter, "_request", fake_request)

    request = OrderRequest(
        account_id="acct",
        symbol="AAPL",
        side="BUY",
        quantity=Decimal("0.1234"),
        price=Decimal("100"),
    )
    await adapter.submit_order(request)

    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload.get("ORD_QTY") == "0.1234"


def test_us_market_open_handles_dst_offsets() -> None:
    edt_time = datetime(2026, 6, 1, 14, 0, tzinfo=timezone.utc)  # 10:00 EDT
    est_time = datetime(2026, 12, 1, 14, 0, tzinfo=timezone.utc)  # 09:00 EST

    assert is_market_open("US", edt_time) is True
    assert is_market_open("US", est_time) is False
    assert is_market_open("US", datetime(2026, 12, 1, 14, 30, tzinfo=timezone.utc)) is True


def test_sec_fee_calculation_with_decimal_precision() -> None:
    with open("configs/fees/us_2026.yaml", encoding="utf-8") as fp:
        fee_cfg = yaml.safe_load(fp)

    sec_rate = Decimal(str(fee_cfg["stocks"]["sell"]["sec_fee_per_dollar"]))
    finra_rate = Decimal(str(fee_cfg["stocks"]["sell"]["finra_taf_per_share"]))
    trade_value = Decimal("12345.67")
    share_qty = Decimal("100")

    sec_fee = trade_value * sec_rate
    finra_fee = share_qty * finra_rate

    assert sec_fee == Decimal("0.09876536")
    assert finra_fee == Decimal("0.0166")
    assert sec_fee + finra_fee == Decimal("0.11536536")


def test_pdt_rule_violation_detection() -> None:
    min_equity = Decimal("25000")
    max_daytrades = 3

    def is_pdt_violation(equity_usd: Decimal, daytrades_last_5d: int) -> bool:
        return equity_usd < min_equity or daytrades_last_5d > max_daytrades

    assert is_pdt_violation(Decimal("28000"), 4) is True
    assert is_pdt_violation(Decimal("25000"), 3) is False
    assert is_pdt_violation(Decimal("24000"), 1) is True
