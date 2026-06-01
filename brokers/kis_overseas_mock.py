"""KIS overseas mock/virtual adapter."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import hashlib
import os

import httpx

from brokers.base import BrokerAdapter, BrokerCapabilities
from brokers.capabilities import KIS_US_MOCK_CAPS
from core.models.market import Symbol
from core.models.order import Fill, OrderAck, OrderRequest, OrderStatus
from core.models.portfolio import Account, CashSnapshot


class KiwoomApiError(RuntimeError):
    """Raised when KIS returns non-zero rt_cd."""

    def __init__(self, rt_cd: str, message: str, payload: dict[str, Any] | None = None) -> None:
        super().__init__(f"KIS API error {rt_cd}: {message}")
        self.rt_cd = rt_cd
        self.message = message
        self.payload = payload or {}


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class _AsyncRateLimiter:
    max_requests_per_second: int = 20
    timestamps: deque[float] = field(default_factory=deque)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    now_fn: Any = field(default=None)
    sleep_fn: Any = field(default=None)

    def __post_init__(self) -> None:
        if self.now_fn is None:
            self.now_fn = asyncio.get_event_loop().time
        if self.sleep_fn is None:
            self.sleep_fn = asyncio.sleep

    async def acquire(self) -> None:
        async with self.lock:
            now = self.now_fn()
            while self.timestamps and now - self.timestamps[0] >= 1.0:
                self.timestamps.popleft()

            if len(self.timestamps) < self.max_requests_per_second:
                self.timestamps.append(now)
                return

            wait = 1.0 - (now - self.timestamps[0])
            await self.sleep_fn(wait)
            now = self.now_fn()
            while self.timestamps and now - self.timestamps[0] >= 1.0:
                self.timestamps.popleft()
            self.timestamps.append(now)


class KISOverseasMockAdapter:
    name: str = "kis_overseas_mock"
    capabilities: BrokerCapabilities = KIS_US_MOCK_CAPS

    def __init__(
        self,
        app_key: str | None = None,
        app_secret: str | None = None,
        account_no: str | None = None,
        base_url: str | None = None,
        rate_limit: int = 20,
    ) -> None:
        self.app_key = app_key or os.getenv("KIS_APP_KEY") or os.getenv("APP_KEY", "")
        self.app_secret = app_secret or os.getenv("KIS_APP_SECRET") or os.getenv("APP_SECRET", "")
        if not self.app_key or not self.app_secret:
            raise ValueError("APP_KEY/APP_SECRET are required")

        self.account_no = account_no or os.getenv("KIS_ACCOUNT_NO", "")
        self.base_url = base_url or "https://openapivts.koreainvestment.com:29443"
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)
        self._rate_limiter = _AsyncRateLimiter(max_requests_per_second=rate_limit)
        self._access_token: str | None = None
        self._access_token_expires_at: datetime | None = None
        self._token_key = hashlib.sha1(f"{self.app_key}:{self.account_no}".encode()).hexdigest()
        self._order_mapping: dict[str, str] = {}
        self._fills: dict[str, list[Fill]] = {}

    def _now(self) -> datetime:
        return _now()

    async def close(self) -> None:
        await self._client.aclose()

    async def _needs_refresh(self) -> bool:
        if self._access_token is None or self._access_token_expires_at is None:
            return True
        return self._now() >= (self._access_token_expires_at - timedelta(minutes=10))

    async def _request_access_token(self) -> None:
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        response = await self._request(
            "POST",
            "/oauth2/tokenP",
            json_data=payload,
            skip_auth=True,
        )
        token = response.get("access_token")
        if not token:
            raise KiwoomApiError("-1", "token response does not include access_token", response)
        expires_in = int(response.get("expires_in", 86400))
        self._access_token = str(token)
        self._access_token_expires_at = self._now() + timedelta(seconds=max(int(expires_in), 600))

    async def _ensure_access_token(self) -> None:
        if await self._needs_refresh():
            await self._request_access_token()

    def _raise_on_error(self, payload: dict[str, Any]) -> None:
        rt_cd = str(payload.get("rt_cd", "0"))
        if rt_cd != "0":
            msg = str(payload.get("msg1") or payload.get("msg") or payload.get("msg_cd") or "rt_cd error")
            raise KiwoomApiError(rt_cd, msg, payload)

    def _common_headers(self) -> dict[str, str]:
        if self._access_token is None:
            return {
                "Content-Type": "application/json",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
            }
        return {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self._access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        tr_id: str | None = None,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        skip_auth: bool = False,
    ) -> dict[str, Any]:
        await self._rate_limiter.acquire()
        await (self._ensure_access_token() if not skip_auth else asyncio.sleep(0))

        headers = self._common_headers()
        if tr_id is not None:
            headers.update({"tr_id": tr_id, "custtype": "P"})

        response = await self._client.request(
            method,
            path,
            params=params,
            json=json_data,
            headers=headers,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise KiwoomApiError("-1", "invalid response payload", {"raw": str(payload)})
        self._raise_on_error(payload)
        return payload

    async def get_market_tick(self, symbol: str | Symbol) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            "/uapi/overseas-price/v1/quotations/price",
            tr_id="HHDFS76200200",
            params={"AUTH": "", "EXCD": "NAS", "SYMB": str(symbol)},
        )
        output = payload.get("output") or payload
        return {
            "symbol": str(symbol),
            "market": "US",
            "currency": "USD",
            "price": _to_decimal(output.get("ovrs_nxt_pric") or output.get("ovrs_prpr") or output.get("pric") or output.get("price")),
            "bid": _to_decimal(output.get("bid") or output.get("ovrs_bid") or output.get("ask") or output.get("price")),
            "ask": _to_decimal(output.get("ask") or output.get("ovrs_ask") or output.get("pric") or output.get("price")),
            "volume": _to_decimal(output.get("acml_vol") or output.get("volume") or "0"),
            "occurred_at": self._now().isoformat(),
        }

    async def get_orderbook(self, symbol: str | Symbol) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            "/uapi/overseas-price/v1/quotations/price",
            tr_id="HHDFS76200200",
            params={"AUTH": "", "EXCD": "NAS", "SYMB": str(symbol)},
        )
        output = payload.get("output") or payload
        return {
            "symbol": str(symbol),
            "bid": _to_decimal(output.get("bid") or output.get("ovrs_bid") or output.get("price")),
            "ask": _to_decimal(output.get("ask") or output.get("ovrs_ask") or output.get("price")),
            "volume": _to_decimal(output.get("acml_vol") or output.get("volume") or "0"),
        }

    async def get_cash_snapshot(self, account_id: str) -> CashSnapshot:
        payload = await self._request(
            "GET",
            "/uapi/overseas-stock/v1/trading/inquire-balance",
            tr_id="VTTS3012R",
            params={"CANO": self.account_no, "ACNT_PRDT_CD": "01", "OVRS_EXCG_CD": "NAS"},
        )
        output = payload.get("output") or payload
        cash = _to_decimal(output.get("dnca_totl_amt") or output.get("cash_amm") or output.get("cash") or "0")
        available = _to_decimal(output.get("dnca_ordable_amt") or output.get("avail_cash") or cash)
        return CashSnapshot(
            account_id=account_id,
            cash_balance=cash,
            available_cash=available,
        )

    def get_account(self, account_id: str) -> Account:
        return Account(account_id=account_id, cash_balance=Decimal("0"), currency="USD")

    async def submit_order(self, request: OrderRequest) -> OrderAck:
        side = request.side.upper()
        if side == "BUY":
            tr_id = "VTTT1002U"
        elif side == "SELL":
            tr_id = "VTTT1001U"
        else:
            raise ValueError(f"unsupported side {request.side}")

        payload: dict[str, Any] = {
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
            broker_order_id = f"KO-{self._token_key[:10]}-{self._now().strftime('%Y%m%d%H%M%S%f')}"

        self._order_mapping[request.order_intent_id] = broker_order_id
        fill = Fill(
            order_id=broker_order_id,
            order_intent_id=request.order_intent_id,
            quantity=request.quantity,
            price=request.price or Decimal("0"),
        )
        self._fills[request.order_intent_id] = [fill]
        return OrderAck(
            order_id=broker_order_id,
            order_intent_id=request.order_intent_id,
            status=OrderStatus.SUBMITTED,
            filled_quantity=Decimal("0"),
            total_quantity=request.quantity,
            fills=[],
        )

    async def get_order_status(self, order_intent_id: str) -> OrderAck | None:
        broker_order_id = self._order_mapping.get(order_intent_id)
        if not broker_order_id:
            return None

        payload = await self._request(
            "GET",
            "/uapi/overseas-stock/v1/trading/inquire-ccnl",
            tr_id="VTTS3035R",
            params={"CANO": self.account_no, "ACNT_PRDT_CD": "01", "OVRS_EXCG_CD": "NAS", "PDNO": "", "CCNL_YN": "N"},
        )
        output = payload.get("output") or payload
        if not isinstance(output, dict):
            output = {}

        filled_quantity = _to_decimal(output.get("ODLN_QTY", "0"))
        total = _to_decimal(output.get("CNCL_QTY", "0")) if filled_quantity else _to_decimal(output.get("ord_qty", "0"))
        if total <= Decimal("0"):
            total = _to_decimal(self._fills.get(order_intent_id, [])[0].quantity if order_intent_id in self._fills else 0)

        status = str(output.get("ODR_ST_CD") or "3001")
        if status in {"3001", "4001", "3900"}:
            order_status = OrderStatus.FILLED
        else:
            order_status = OrderStatus.BROKER_ACKED

        fills = self._fills.get(order_intent_id, [])
        return OrderAck(
            order_id=broker_order_id,
            order_intent_id=order_intent_id,
            status=order_status,
            filled_quantity=filled_quantity if filled_quantity > Decimal("0") else (fills[0].quantity if fills else Decimal("0")),
            total_quantity=total or (fills[0].quantity if fills else Decimal("1")),
            average_fill_price=(_to_decimal(output.get("OD_UNPR")) if isinstance(output, dict) else Decimal("0")),
            fills=fills,
        )

    async def get_fx_rate(self) -> Decimal:
        payload = await self._request(
            "GET",
            "/uapi/overseas-price/v1/quotations/inquire-daily-fcblprice",
            tr_id="HHKDB80100100",
            params={"AUTH": "", "RSYM": "USD"},
        )
        output = payload.get("output") or payload
        if isinstance(output, dict):
            return _to_decimal(output.get("famt") or output.get("FXRATE") or output.get("exchange_rate", "0"))
        return Decimal("0")


# runtime protocol compatibility alias
BrokerAdapter = BrokerAdapter
