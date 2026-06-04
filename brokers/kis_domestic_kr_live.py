"""KIS domestic KR live adapter for production credentials."""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, AsyncIterator
import hashlib
import json
import os

import httpx
import websockets

from brokers.base import BrokerAdapter, BrokerCapabilities
from brokers.capabilities import KIS_KR_LIVE_CAPS
from core.models.market import Symbol
from core.models.order import Fill, OrderAck, OrderRequest, OrderStatus
from core.models.portfolio import Account, CashSnapshot


class KISDomesticApiError(RuntimeError):
    """Raised when KIS API returns non-zero rt_cd."""

    def __init__(self, rt_cd: str, message: str, payload: dict[str, Any] | None = None) -> None:
        super().__init__(f"KIS API error {rt_cd}: {message}")
        self.rt_cd = rt_cd
        self.message = message
        self.payload = payload or {}


@dataclass
class _AsyncRateLimiter:
    max_requests_per_second: int = 5
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


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _now() -> datetime:
    return datetime.now(timezone.utc)


class KISDomesticKrLiveAdapter:
    """Live KIS domestic adapter with the live credential naming convention."""

    name: str = "kis_domestic_kr_live"
    capabilities: BrokerCapabilities = KIS_KR_LIVE_CAPS

    def __init__(
        self,
        app_key: str | None = None,
        app_secret: str | None = None,
        account_no: str | None = None,
        base_url: str | None = None,
        rate_limit: int = 5,
    ) -> None:
        self.app_key = app_key or os.getenv("KIS_LIVE_APP_KEY") or os.getenv("KIS_APP_KEY", "")
        self.app_secret = app_secret or os.getenv("KIS_LIVE_APP_SECRET") or os.getenv("KIS_APP_SECRET", "")
        if not self.app_key or not self.app_secret:
            raise ValueError("APP_KEY/APP_SECRET are required")
        self.account_no = account_no or os.getenv("KIS_LIVE_ACCOUNT_NO") or os.getenv("KIS_ACCOUNT_NO", "")
        self.base_url = base_url or "https://openapi.koreainvestment.com:9443"
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)
        self._rate_limiter = _AsyncRateLimiter(max_requests_per_second=rate_limit)
        self._access_token: str | None = None
        self._access_token_expires_at: datetime | None = None
        self._token_key = hashlib.sha1(f"{self.app_key}:{self.account_no}".encode()).hexdigest()
        self._order_mapping: dict[str, str] = {}
        self._positions: dict[str, Decimal] = {}

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
            raise KISDomesticApiError("-1", "token response does not include access_token", response)
        expires_in = int(response.get("expires_in", 86400))
        self._access_token = str(token)
        self._access_token_expires_at = self._now() + timedelta(seconds=max(int(expires_in), 600))

    async def _ensure_access_token(self) -> None:
        if await self._needs_refresh():
            await self._request_access_token()

    def _raise_on_error(self, payload: dict[str, Any]) -> None:
        rt_cd = str(payload.get("rt_cd", "0"))
        if rt_cd != "0":
            msg = str(payload.get("msg1") or payload.get("msg2") or payload.get("msg") or "rt_cd error")
            raise KISDomesticApiError(rt_cd, msg, payload)

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
            raise KISDomesticApiError("-1", "invalid response payload", {"raw": str(payload)})
        self._raise_on_error(payload)
        return payload

    async def get_market_tick(self, symbol: str | Symbol) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            tr_id="FHKST01010100",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": str(symbol)},
        )
        output = payload.get("output") or {}
        return {
            "symbol": str(symbol),
            "price": _to_decimal(output.get("stck_prpr") or output.get("price") or output.get("output2", {}).get("stck_prpr")),
            "bid": _to_decimal(output.get("stck_bfqy") or output.get("bid") or output.get("bstp_nmix_prpr")),
            "ask": _to_decimal(output.get("stck_sdqy") or output.get("ask") or output.get("ovrs_prpr")),
            "volume": _to_decimal(output.get("acml_vol") or output.get("volume") or "0"),
            "occurred_at": self._now().isoformat(),
        }

    async def get_orderbook(self, symbol: str | Symbol) -> dict[str, Any]:
        payload = await self._request(
            "GET",
            "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
            tr_id="FHKST01010300",
            params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": str(symbol)},
        )
        output = payload.get("output") or {}
        return {
            "symbol": str(symbol),
            "bid": _to_decimal(output.get("bidp1") or output.get("bid")),
            "ask": _to_decimal(output.get("askp1") or output.get("ask")),
            "volume": _to_decimal(output.get("acml_vol") or "0"),
        }

    async def submit_order(self, request: OrderRequest) -> OrderAck:
        side = request.side.upper()
        if side == "BUY":
            tr_id = "TTTC0802U"
        elif side == "SELL":
            tr_id = "TTTC0801U"
        else:
            raise ValueError(f"unsupported side {request.side}")

        payload: dict[str, Any] = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": "01",
            "PDNO": request.symbol,
            "ORD_DVSN": "01",
            "ORD_QTY": str(int(request.quantity)),
            "ORD_UNPR": str(int(request.price or Decimal("0"))) if request.price else "0",
        }

        response = await self._request(
            "POST",
            "/uapi/domestic-stock/v1/trading/order-cash",
            tr_id=tr_id,
            json_data=payload,
        )

        output = response.get("output") or response
        broker_order_id = str(output.get("ODNO") or output.get("order_no") or output.get("orderId") or "")
        if not broker_order_id:
            broker_order_id = f"KO-{self._token_key[:10]}-{_now().strftime('%Y%m%d%H%M%S%f')}"

        self._order_mapping[request.order_intent_id] = broker_order_id
        return OrderAck(
            order_id=broker_order_id,
            order_intent_id=request.order_intent_id,
            status=OrderStatus.SUBMITTED,
            filled_quantity=Decimal("0"),
            total_quantity=request.quantity,
            average_fill_price=request.price,
        )

    async def cancel_order(self, account_id: str, broker_order_id: str) -> dict[str, Any]:
        payload = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": "01",
            "PDNO": "",
            "ODNO": broker_order_id,
        }
        response = await self._request(
            "POST",
            "/uapi/domestic-stock/v1/trading/order-rvsecncl",
            tr_id="TTSC0803U",
            json_data=payload,
        )
        output = response.get("output") or {}
        return {
            "order_id": broker_order_id,
            "account_id": account_id,
            "result": output,
        }

    async def get_order_status(self, order_intent_id: str) -> OrderAck | None:
        broker_order_id = self._order_mapping.get(order_intent_id)
        if not broker_order_id:
            return None

        payload = await self._request(
            "GET",
            "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            tr_id="TTTC8001R",
            params={
                "CANO": self.account_no,
                "ACNT_PRDT_CD": "01",
                "INQR_STRT_DT": self._now().strftime("%Y%m%d"),
                "INQR_END_DT": self._now().strftime("%Y%m%d"),
            },
        )
        rows = payload.get("output2") or payload.get("output") or []
        if not isinstance(rows, list):
            rows = [rows]

        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("ODNO", "")) != broker_order_id:
                continue

            filled = _to_decimal(row.get("CNCL_QTY") or row.get("total_qty") or "0")
            status_text = str(row.get("OD_STS") or "submitted").upper()
            status = OrderStatus.SUBMITTED
            if "CANCEL" in status_text:
                status = OrderStatus.REJECTED
            elif status_text in {"F", "PARTIALLY_FILLED"}:
                ordered_qty = _to_decimal(row.get("ORD_QTY") or row.get("qty") or "0")
                status = OrderStatus.PARTIALLY_FILLED if filled < ordered_qty else OrderStatus.FILLED

            fill_price = _to_decimal(row.get("AVG_PRC") or row.get("price") or "0")
            return OrderAck(
                order_id=broker_order_id,
                order_intent_id=order_intent_id,
                status=status,
                filled_quantity=filled,
                total_quantity=_to_decimal(row.get("ORD_QTY") or row.get("qty") or "0"),
                average_fill_price=fill_price,
                fills=[
                    Fill(
                        order_id=broker_order_id,
                        order_intent_id=order_intent_id,
                        quantity=filled,
                        price=fill_price,
                    )
                ],
            )

        return None

    async def get_cash_snapshot(self, account_id: str) -> CashSnapshot:
        payload = await self._request(
            "GET",
            "/uapi/domestic-stock/v1/trading/inquire-balance",
            tr_id="TTTC8434R",
            params={"CANO": self.account_no, "ACNT_PRDT_CD": "01"},
        )
        output = payload.get("output") or payload
        cash = _to_decimal(output.get("dnca_totl_amt") or output.get("cash_amount") or output.get("cash") or "0")
        available = _to_decimal(output.get("dnca_ordable_amt") or output.get("available_cash") or cash)
        return CashSnapshot(
            account_id=account_id,
            cash_balance=cash,
            available_cash=available,
        )

    async def get_positions(self, account_id: str) -> dict[str, Decimal]:
        # KIS domestic position endpoint is not fully modeled in this skeleton.
        # Keep a compatibility surface for monitors; use internal cache when available.
        if not self._positions:
            return {}
        return {str(symbol): qty for symbol, qty in self._positions.items()}

    def _extract_market(self, body: Any) -> dict[str, Any] | None:
        if not isinstance(body, dict):
            return None

        if "output" in body and isinstance(body["output"], dict):
            body = body["output"]

        symbol = body.get("symbol") or body.get("mksc_shrn_iscd") or body.get("pdno")
        if symbol is None:
            return None

        return {
            "symbol": str(symbol),
            "market": "KR",
            "price": _to_decimal(body.get("stck_prpr") or body.get("prc") or body.get("price")),
            "volume": _to_decimal(body.get("acml_tr_pbmn") or body.get("vol") or "0"),
            "occurred_at": self._now().isoformat(),
        }

    async def stream_market_ticks(self, symbols: list[str]) -> AsyncIterator[dict[str, Any]]:
        symbols_csv = ",".join(symbols)
        while True:
            try:
                async with websockets.connect("wss://openapi.koreainvestment.com:9443/websocket/ordr/H0STCNT0") as ws:
                    subscribe = {
                        "header": {
                            "tr_id": "H0STCNT0",
                            "custtype": "P",
                            "content-type": "utf-8",
                        },
                        "body": {
                            "input": {
                                "tr_type": "1",
                                "tr_id": "H0STCNT0",
                                "custtype": "P",
                                "symbols": symbols_csv,
                            }
                        },
                    }
                    await ws.send(json.dumps(subscribe))
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        payload = json.loads(raw)
                        item = self._extract_market(payload)
                        if item is not None:
                            yield item
            except Exception:
                await asyncio.sleep(1)

    async def stream_fills(self, account_id: str | None = None) -> AsyncIterator[dict[str, Any]]:
        while True:
            try:
                async with websockets.connect("wss://openapi.koreainvestment.com:9443/websocket/ordr/H0STCNI0") as ws:
                    await ws.send(json.dumps({"header": {"tr_id": "H0STCNI0", "custtype": "P"}}))
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        payload = json.loads(raw)
                        body = payload.get("body") if isinstance(payload, dict) else None
                        if isinstance(body, dict):
                            yield body
            except Exception:
                await asyncio.sleep(1)

    def get_account(self, account_id: str) -> Account:
        return Account(account_id=account_id, cash_balance=Decimal("0"))


# runtime protocol compatibility alias
BrokerAdapter = BrokerAdapter
