"""KIS overseas live adapter."""

from __future__ import annotations

from decimal import Decimal
import os

from brokers.kis_overseas_mock import (
    KiwoomApiError,
    KISOverseasMockAdapter,
    _to_decimal,
)
from brokers.capabilities import KIS_US_LIVE_CAPS


class KISOverseasLiveAdapter(KISOverseasMockAdapter):
    name: str = "kis_overseas_live"
    capabilities = KIS_US_LIVE_CAPS

    def __init__(
        self,
        app_key: str | None = None,
        app_secret: str | None = None,
        account_no: str | None = None,
        base_url: str | None = None,
        rate_limit: int = 20,
    ) -> None:
        super().__init__(
            app_key=app_key or os.getenv("KIS_LIVE_APP_KEY"),
            app_secret=app_secret or os.getenv("KIS_LIVE_APP_SECRET"),
            account_no=account_no or os.getenv("KIS_LIVE_ACCOUNT_NO"),
            base_url=base_url or "https://openapi.koreainvestment.com:9443",
            rate_limit=rate_limit,
        )

    async def submit_order(self, request):  # type: ignore[override]
        side = request.side.upper()
        if side == "BUY":
            tr_id = "TTTT1002U"
        elif side == "SELL":
            tr_id = "TTTT1001U"
        else:
            raise ValueError(f"unsupported side {request.side}")

        payload = {
            "CANO": self.account_no,
            "OVRS_EXCG_CD": "NAS",
            "PDNO": request.symbol,
            "ORD_QTY": str(request.quantity),
            "OVRS_ORD_UNPR": str(request.price) if request.price is not None else "0",
        }

        response = await self._request(
            "POST",
            "/uapi/overseas-stock/v1/trading/order",
            tr_id=tr_id,
            json_data=payload,
        )
        output = response.get("output") or response
        broker_order_id = str(output.get("ODNO") or output.get("order_no") or output.get("orderId") or "")
        if not broker_order_id:
            broker_order_id = await self._fallback_order_id()
        self._order_mapping[request.order_intent_id] = broker_order_id
        fill = self._fills.get(request.order_intent_id, [])
        if not fill:
            from core.models.order import Fill

            fill = [
                Fill(
                    order_id=broker_order_id,
                    order_intent_id=request.order_intent_id,
                    quantity=request.quantity,
                    price=request.price or _to_decimal("0"),
                )
            ]
            self._fills[request.order_intent_id] = fill

        from core.models.order import OrderAck, OrderStatus

        return OrderAck(
            order_id=broker_order_id,
            order_intent_id=request.order_intent_id,
            status=OrderStatus.SUBMITTED,
            filled_quantity=fill[0].quantity if fill else _to_decimal("0"),
            total_quantity=request.quantity,
            fills=fill,
        )

    async def get_order_status(self, order_intent_id: str):  # type: ignore[override]
        broker_order_id = self._order_mapping.get(order_intent_id)
        if not broker_order_id:
            return None

        from core.models.order import OrderAck, OrderStatus
        response = await self._request(
            "GET",
            "/uapi/overseas-stock/v1/trading/inquire-ccnl",
            tr_id="TTTS3035R",
            params={"CANO": self.account_no, "ACNT_PRDT_CD": "01", "OVRS_EXCG_CD": "NAS", "PDNO": "", "CCNL_YN": "N"},
        )
        output = response.get("output") or response
        if not isinstance(output, dict):
            return None

        status = str(output.get("ODR_ST_CD") or "3001")
        mapped = OrderStatus.FILLED if status in {"3001", "4001", "3900"} else OrderStatus.BROKER_ACKED
        filled = _to_decimal(output.get("ODLN_QTY", "0"))
        total = _to_decimal(output.get("ORD_QTY", "0"))
        fills = self._fills.get(order_intent_id, [])
        return OrderAck(
            order_id=broker_order_id,
            order_intent_id=order_intent_id,
            status=mapped,
            filled_quantity=filled if filled else (fills[0].quantity if fills else _to_decimal("0")),
            total_quantity=total if total else _to_decimal("1"),
            average_fill_price=_to_decimal(output.get("OD_PRC", "0")),
            fills=fills,
        )

    async def get_cash_snapshot(self, account_id: str):
        payload = await self._request(
            "GET",
            "/uapi/overseas-stock/v1/trading/inquire-balance",
            tr_id="TTTS3012R",
            params={"CANO": self.account_no, "ACNT_PRDT_CD": "01", "OVRS_EXCG_CD": "NAS"},
        )
        output = payload.get("output") or payload
        from core.models.portfolio import CashSnapshot

        cash = _to_decimal(output.get("dnca_totl_amt") or output.get("cash_amm") or output.get("cash") or "0")
        available = _to_decimal(output.get("dnca_ordable_amt") or output.get("avail_cash") or cash)
        return CashSnapshot(
            account_id=account_id,
            cash_balance=cash,
            available_cash=available,
        )

    async def get_fx_rate(self) -> Decimal:
        return await super().get_fx_rate()

    async def _fallback_order_id(self) -> str:
        return f"KO-LIVE-{self._token_key[:10]}-{self._now().strftime('%Y%m%d%H%M%S%f')}"


KiwoomApiError = KiwoomApiError
