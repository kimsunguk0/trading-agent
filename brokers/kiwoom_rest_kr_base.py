"""Kiwoom REST OpenAPI adapter for Korean domestic stocks."""

from __future__ import annotations

import asyncio
import json
import os
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, AsyncIterator
from zoneinfo import ZoneInfo

import httpx

try:
    import websockets
except Exception:  # pragma: no cover
    websockets = None

from brokers.base import BrokerCapabilities
from core.models.market import Symbol
from core.models.order import Fill, OrderAck, OrderRequest, OrderStatus
from core.models.portfolio import Account, CashSnapshot, Position


KIWOOM_MOCK_BASE_URL = "https://mockapi.kiwoom.com"
KIWOOM_LIVE_BASE_URL = "https://api.kiwoom.com"
KIWOOM_MOCK_WS_URL = "wss://mockapi.kiwoom.com:10000/api/dostk/websocket"
KIWOOM_LIVE_WS_URL = "wss://api.kiwoom.com:10000/api/dostk/websocket"


class KiwoomApiError(RuntimeError):
    """Raised when Kiwoom REST OpenAPI returns an error payload."""

    def __init__(self, code: str, message: str, payload: dict[str, Any] | None = None) -> None:
        super().__init__(f"Kiwoom API error {code}: {message}")
        self.code = code
        self.rt_cd = code
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


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dec(value: Any) -> Decimal:
    """Parse Kiwoom zero-padded numeric strings into Decimal."""

    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    text = str(value).strip().replace(",", "")
    if not text:
        return Decimal("0")
    return Decimal(text)


def _parse_num(value: Any) -> int | Decimal:
    """Parse Kiwoom numeric strings, returning int unless a decimal point exists."""

    number = _parse_dec(value)
    if "." in str(value or ""):
        return number
    return int(number)


def _parse_price(value: Any) -> Decimal:
    """Parse Kiwoom quote prices where +/- is direction, not signed value."""

    text = str(value or "").strip()
    if text in {"", "0", "-0", "+0"}:
        return Decimal("0")
    return Decimal(text.lstrip("+-").lstrip("0") or "0")


def _strip_code(value: Any) -> str:
    """Normalize Kiwoom stock codes such as A005930 -> 005930."""

    code = str(value or "").strip()
    while code and code[0].isalpha():
        code = code[1:]
    return code


def _to_decimal(value: Any) -> Decimal:
    return _parse_dec(value)


def _decimal_string(value: Decimal | int | str | None) -> str:
    if value is None:
        return ""
    number = _parse_dec(value)
    if number == number.to_integral_value():
        return str(int(number))
    return format(number.normalize(), "f")


def _first(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return value
    return default


def _as_list(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _parse_kiwoom_expires_dt(value: Any) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        return _now() + timedelta(hours=23)
    try:
        expires = datetime.strptime(raw, "%Y%m%d%H%M%S")
        return expires.replace(tzinfo=ZoneInfo("Asia/Seoul")).astimezone(timezone.utc)
    except Exception:
        # TODO(openapi.kiwoom.com): confirm fallback behavior for malformed expires_dt.
        return _now() + timedelta(hours=23)


class KiwoomRestKrBaseAdapter:
    """Common Kiwoom REST OpenAPI implementation.

    Confirmed request contracts are implemented exactly. Some response field
    names are parsed defensively because the public examples vary by endpoint.
    """

    name: str = "kiwoom_rest_kr"
    capabilities: BrokerCapabilities
    default_base_url: str = KIWOOM_MOCK_BASE_URL
    default_websocket_url: str = KIWOOM_MOCK_WS_URL

    def __init__(
        self,
        app_key: str | None = None,
        app_secret: str | None = None,
        account_no: str | None = None,
        base_url: str | None = None,
        websocket_url: str | None = None,
        rate_limit: int = 5,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.app_key = app_key or os.getenv("KIWOOM_APP_KEY", "")
        self.app_secret = app_secret or os.getenv("KIWOOM_APP_SECRET", "")
        self.account_no = account_no or os.getenv("KIWOOM_ACCOUNT_NO", "")
        if not self.app_key or not self.app_secret:
            raise ValueError("KIWOOM_APP_KEY/KIWOOM_APP_SECRET are required")
        if not self.account_no:
            raise ValueError("KIWOOM_ACCOUNT_NO is required")

        self.base_url = (base_url or os.getenv("KIWOOM_BASE_URL") or self.default_base_url).rstrip("/")
        self.websocket_url = websocket_url or os.getenv("KIWOOM_WS_URL") or os.getenv("KIWOOM_WEBSOCKET_URL") or self._derive_websocket_url()
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0, transport=transport)
        self._rate_limiter = _AsyncRateLimiter(max_requests_per_second=rate_limit)
        self._access_token: str | None = None
        self._access_token_expires_at: datetime | None = None
        self._order_mapping: dict[str, str] = {}
        self._last_order_ack: dict[str, OrderAck] = {}
        self._last_position_summary: dict[str, Decimal] = {}

    def _now(self) -> datetime:
        return _now()

    def _derive_websocket_url(self) -> str:
        try:
            host = httpx.URL(self.base_url).host
        except Exception:
            host = None
        if not host:
            return self.default_websocket_url
        return f"wss://{host}:10000/api/dostk/websocket"

    async def close(self) -> None:
        await self._client.aclose()

    async def _needs_refresh(self) -> bool:
        if self._access_token is None or self._access_token_expires_at is None:
            return True
        return self._now() >= (self._access_token_expires_at - timedelta(minutes=10))

    async def _request_access_token(self) -> None:
        await self._rate_limiter.acquire()
        response = await self._client.post(
            "/oauth2/token",
            json={
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "secretkey": self.app_secret,
            },
            headers={"Content-Type": "application/json;charset=UTF-8"},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise KiwoomApiError("-1", "invalid token response payload", {"raw": str(payload)})
        self._raise_on_error(payload)

        token = payload.get("token")
        if not token:
            raise KiwoomApiError("-1", "token response does not include token", payload)
        self._access_token = str(token)
        self._access_token_expires_at = _parse_kiwoom_expires_dt(payload.get("expires_dt"))

    async def revoke_token(self) -> dict[str, Any]:
        if self._access_token is None:
            return {"return_code": 0, "return_msg": "no token"}
        await self._rate_limiter.acquire()
        response = await self._client.post(
            "/oauth2/revoke",
            json={"appkey": self.app_key, "secretkey": self.app_secret, "token": self._access_token},
            headers={"Content-Type": "application/json;charset=UTF-8"},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise KiwoomApiError("-1", "invalid revoke response payload", {"raw": str(payload)})
        self._raise_on_error(payload)
        self._access_token = None
        self._access_token_expires_at = None
        return payload

    async def _ensure_access_token(self) -> None:
        if await self._needs_refresh():
            await self._request_access_token()

    def _raise_on_error(self, payload: dict[str, Any]) -> None:
        if "return_code" in payload and str(payload.get("return_code")) not in {"0", ""}:
            raise KiwoomApiError(str(payload.get("return_code")), str(payload.get("return_msg") or "return_code error"), payload)
        if "rt_cd" in payload and str(payload.get("rt_cd")) not in {"0", ""}:
            raise KiwoomApiError(str(payload.get("rt_cd")), str(payload.get("msg1") or payload.get("msg") or "rt_cd error"), payload)

    def _headers(self, api_id: str, *, cont_yn: str = "N", next_key: str = "") -> dict[str, str]:
        if not self._access_token:
            raise KiwoomApiError("-1", "access token is not initialized")
        return {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {self._access_token}",
            "api-id": api_id,
            "cont-yn": cont_yn,
            "next-key": next_key,
        }

    async def _post_api_with_headers(
        self,
        path: str,
        api_id: str,
        body: dict[str, Any] | None = None,
        *,
        cont_yn: str = "N",
        next_key: str = "",
    ) -> tuple[dict[str, Any], httpx.Headers]:
        await self._rate_limiter.acquire()
        await self._ensure_access_token()
        response = await self._client.post(
            path,
            json=body or {},
            headers=self._headers(api_id, cont_yn=cont_yn, next_key=next_key),
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise KiwoomApiError("-1", "invalid response payload", {"raw": str(payload)})
        self._raise_on_error(payload)
        return payload, response.headers

    async def _post_api(self, path: str, api_id: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        payload, _headers = await self._post_api_with_headers(path, api_id, body)
        return payload

    async def _post_api_pages(self, path: str, api_id: str, body: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        pages: list[dict[str, Any]] = []
        cont_yn = "N"
        next_key = ""
        while True:
            payload, headers = await self._post_api_with_headers(path, api_id, body, cont_yn=cont_yn, next_key=next_key)
            pages.append(payload)
            cont_yn = str(headers.get("cont-yn") or headers.get("cont_yn") or "N").upper()
            next_key = str(headers.get("next-key") or headers.get("next_key") or "")
            if cont_yn != "Y" or not next_key:
                return pages

    def _order_type_fields(self, request: OrderRequest) -> tuple[str, str]:
        order_type = str(request.order_type or "").upper()
        if order_type == "MARKET" or request.price is None:
            return "3", ""
        return "0", _decimal_string(request.price)

    async def get_accounts(self) -> list[str]:
        return [self.account_no]

    async def get_cash(self, account_id: str | None = None) -> CashSnapshot:
        _ = account_id
        payload = await self._post_api(
            "/api/dostk/acnt",
            "kt00001",
            {"qry_tp": "1"},
        )
        body = payload.get("output") if isinstance(payload.get("output"), dict) else payload
        cash = _parse_dec(_first(body, "entr", "dnca_totl_amt", "cash_balance", "cash", "예수금", default="0"))
        available = _parse_dec(_first(body, "ord_alow_amt", "ord_psbl_amt", "available_cash", "주문가능금액", default=cash))
        return CashSnapshot(account_id=account_id or self.account_no, cash_balance=cash, available_cash=available)

    async def get_cash_snapshot(self, account_id: str) -> CashSnapshot:
        return await self.get_cash(account_id)

    async def get_positions(self, account_id: str | None = None) -> dict[str, dict[str, Any]]:
        _ = account_id
        rows: list[dict[str, Any]] = []
        pages = await self._post_api_pages(
            "/api/dostk/acnt",
            "kt00018",
            {"qry_tp": "1", "dmst_stex_tp": "KRX"},
        )
        for page in pages:
            self._last_position_summary = {
                "total_purchase_amount": _parse_dec(page.get("tot_pur_amt")),
                "total_evaluation_amount": _parse_dec(page.get("tot_evlt_amt")),
                "total_evaluation_pnl": _parse_dec(page.get("tot_evlt_pl")),
                "total_profit_rate": _parse_dec(page.get("tot_prft_rt")),
                "estimated_deposit_asset_amount": _parse_dec(page.get("prsm_dpst_aset_amt")),
            }
            candidates = (
                page.get("acnt_evlt_remn_indv_tot"),
                page.get("stk_acnt_evlt_prstn"),
            )
            for candidate in candidates:
                rows.extend(_as_list(candidate))

        positions: dict[str, dict[str, Any]] = {}
        for row in rows:
            symbol = _strip_code(_first(row, "stk_cd", "pdno", "symbol", "종목코드", default=""))
            if not symbol:
                continue
            qty = _parse_dec(_first(row, "rmnd_qty", "hldg_qty", "ord_psbl_qty", "quantity", "보유수량", default="0"))
            if qty != 0:
                positions[symbol] = {
                    "symbol": symbol,
                    "name": str(_first(row, "stk_nm", "name", "종목명", default="")),
                    "quantity": qty,
                    "tradeable_quantity": _parse_dec(_first(row, "trde_able_qty", "ord_psbl_qty", default="0")),
                    "average_price": _parse_dec(_first(row, "pur_pric", "average_price", default="0")),
                    "current_price": _parse_dec(_first(row, "cur_prc", "current_price", default="0")),
                    "unrealized_pnl": _parse_dec(_first(row, "evltv_prft", "evlt_prft", "unrealized_pnl", default="0")),
                    "profit_rate": _parse_dec(_first(row, "prft_rt", "profit_rate", default="0")),
                    "evaluation_amount": _parse_dec(_first(row, "evlt_amt", "evaluation_amount", default="0")),
                    "purchase_amount": _parse_dec(_first(row, "pur_amt", "purchase_amount", default="0")),
                    "purchase_commission": _parse_dec(_first(row, "pur_cmsn", default="0")),
                    "sell_commission": _parse_dec(_first(row, "sell_cmsn", default="0")),
                    "tax": _parse_dec(_first(row, "tax", default="0")),
                    "total_commission": _parse_dec(_first(row, "sum_cmsn", default="0")),
                    "raw": row,
                }
        return positions

    async def get_account(self, account_id: str) -> Account:
        cash = await self.get_cash(account_id)
        raw_positions = await self.get_positions(account_id)
        positions = {
            symbol: Position(
                account_id=account_id,
                symbol=symbol,
                quantity=_parse_dec(item.get("quantity")),
                average_price=_parse_dec(item.get("average_price")),
                realized_pnl=_parse_dec(item.get("unrealized_pnl")),
            )
            for symbol, item in raw_positions.items()
        }
        return Account(account_id=account_id, cash_balance=cash.cash_balance, positions=positions)

    async def get_quote(self, symbol: str | Symbol) -> dict[str, Any]:
        code = _strip_code(symbol)
        payload = await self._post_api("/api/dostk/stkinfo", "ka10001", {"stk_cd": code})
        body = payload.get("output") if isinstance(payload.get("output"), dict) else payload
        return {
            "symbol": _strip_code(_first(body, "stk_cd", default=code)),
            "name": str(_first(body, "stk_nm", default="")),
            "market": "KR",
            "price": _parse_price(_first(body, "cur_prc", default="0")),
            "prev_close": _parse_price(_first(body, "base_pric", default="0")),
            "open": _parse_price(_first(body, "open_pric", default="0")),
            "high": _parse_price(_first(body, "high_pric", default="0")),
            "low": _parse_price(_first(body, "low_pric", default="0")),
            "upper_limit": _parse_price(_first(body, "upl_pric", default="0")),
            "lower_limit": _parse_price(_first(body, "lst_pric", default="0")),
            "change": _parse_dec(_first(body, "pred_pre", default="0")),
            "change_rate": _parse_dec(_first(body, "flu_rt", default="0")),
            "volume": int(_parse_num(_first(body, "trde_qty", default="0"))),
            "is_halted": str(_first(body, "trde_stop_yn", "is_halted", default="N")).upper() in {"Y", "TRUE", "1"},
            "occurred_at": self._now().isoformat(),
            "raw": payload,
        }

    async def get_market_tick(self, symbol: str | Symbol) -> dict[str, Any]:
        return await self.get_quote(symbol)

    async def get_orderbook(self, symbol: str | Symbol) -> dict[str, Any]:
        code = _strip_code(symbol)
        payload = await self._post_api("/api/dostk/mrkcond", "ka10004", {"stk_cd": code})
        body = payload.get("output") if isinstance(payload.get("output"), dict) else payload
        asks: list[dict[str, Decimal | int]] = []
        bids: list[dict[str, Decimal | int]] = []
        for level in range(1, 11):
            if level == 1:
                ask_price_key = "sel_fpr_bid"
                ask_qty_key = "sel_fpr_req"
                bid_price_key = "buy_fpr_bid"
                bid_qty_key = "buy_fpr_req"
            else:
                ask_price_key = f"sel_{level}th_pre_bid"
                ask_qty_key = f"sel_{level}th_pre_req"
                bid_price_key = f"buy_{level}th_pre_bid"
                bid_qty_key = f"buy_{level}th_pre_req"

            asks.append(
                {
                    "price": _parse_price(_first(body, ask_price_key, default="0")),
                    "quantity": int(_parse_num(_first(body, ask_qty_key, default="0"))),
                }
            )
            bids.append(
                {
                    "price": _parse_price(_first(body, bid_price_key, default="0")),
                    "quantity": int(_parse_num(_first(body, bid_qty_key, default="0"))),
                }
            )

        asks.sort(key=lambda item: item["price"])
        bids.sort(key=lambda item: item["price"], reverse=True)
        return {
            "symbol": code,
            "asks": asks,
            "bids": bids,
            "best_ask": asks[0] if asks else None,
            "best_bid": bids[0] if bids else None,
            "ask": asks[0]["price"] if asks else Decimal("0"),
            "bid": bids[0]["price"] if bids else Decimal("0"),
            "total_ask_qty": int(_parse_num(_first(body, "tot_sel_req", default="0"))),
            "total_bid_qty": int(_parse_num(_first(body, "tot_buy_req", default="0"))),
            "ts": str(_first(body, "bid_req_base_tm", default="")),
            "raw": payload,
        }

    async def submit_order(self, request: OrderRequest) -> OrderAck:
        side = request.side.upper()
        if side == "BUY":
            api_id = "kt10000"
        elif side == "SELL":
            api_id = "kt10001"
        else:
            raise ValueError(f"unsupported side {request.side}")

        trde_tp, ord_uv = self._order_type_fields(request)
        extra = getattr(request, "model_extra", None)
        if not isinstance(extra, dict):
            extra = {}
        payload = {
            "dmst_stex_tp": str(extra.get("dmst_stex_tp") or "KRX"),
            "stk_cd": _strip_code(request.symbol),
            "ord_qty": _decimal_string(request.quantity),
            "ord_uv": ord_uv,
            "trde_tp": trde_tp,
        }
        response = await self._post_api("/api/dostk/ordr", api_id, payload)
        broker_order_id = str(_first(response, "ord_no", "order_no", "ordNo", default="")).strip()
        if not broker_order_id and isinstance(response.get("output"), dict):
            broker_order_id = str(_first(response["output"], "ord_no", "order_no", "ordNo", default="")).strip()
        if not broker_order_id:
            raise KiwoomApiError("-1", "order response does not include ord_no", response)

        ack = OrderAck(
            order_id=broker_order_id,
            order_intent_id=request.order_intent_id,
            status=OrderStatus.SUBMITTED,
            filled_quantity=Decimal("0"),
            total_quantity=request.quantity,
            average_fill_price=request.price,
        )
        self._order_mapping[request.order_intent_id] = broker_order_id
        self._last_order_ack[request.order_intent_id] = ack
        return ack

    async def place_order(self, request: OrderRequest) -> OrderAck:
        return await self.submit_order(request)

    async def cancel_order(
        self,
        account_id: str,
        broker_order_id: str,
        quantity: Decimal | int | str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "orig_ord_no": broker_order_id,
            # TODO(openapi.kiwoom.com): confirm exact cancel quantity field for kt10003.
            "cncl_qty": _decimal_string(quantity),
        }
        response = await self._post_api("/api/dostk/ordr", "kt10003", payload)
        return {"order_id": broker_order_id, "account_id": account_id, "result": response}

    async def modify_order(
        self,
        account_id: str,
        broker_order_id: str,
        *,
        quantity: Decimal | int | str | None = None,
        price: Decimal | int | str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "orig_ord_no": broker_order_id,
            # TODO(openapi.kiwoom.com): confirm exact modify fields for kt10002.
            "mdfy_qty": _decimal_string(quantity),
            "mdfy_uv": _decimal_string(price),
        }
        response = await self._post_api("/api/dostk/ordr", "kt10002", payload)
        return {"order_id": broker_order_id, "account_id": account_id, "result": response}

    async def get_order_status(self, order_intent_id: str) -> OrderAck | None:
        # TODO(openapi.kiwoom.com): confirm official REST order-status TR/path.
        # Fills are expected through WebSocket type 0B; keep last order ack for
        # execution-worker compatibility until the polling TR is confirmed.
        return self._last_order_ack.get(order_intent_id)

    async def is_tradable(self, symbol: str | Symbol) -> bool:
        quote = await self.get_quote(symbol)
        if bool(quote.get("is_halted")):
            return False
        return _to_decimal(quote.get("price")) > Decimal("0")

    def _parse_ws_payload(self, raw: str | bytes) -> dict[str, Any] | None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            payload = json.loads(raw)
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    def _check_ws_response(self, frame: dict[str, Any] | None, expected_trnm: str) -> None:
        if frame is None:
            raise KiwoomApiError("-1", f"invalid {expected_trnm} websocket response")
        trnm = str(frame.get("trnm") or "")
        if trnm != expected_trnm:
            raise KiwoomApiError("-1", f"unexpected websocket response {trnm}, expected {expected_trnm}", frame)
        if "return_code" in frame and str(frame.get("return_code")) not in {"0", ""}:
            raise KiwoomApiError(str(frame.get("return_code")), str(frame.get("return_msg") or "websocket error"), frame)

    def _parse_real_trade(self, item: dict[str, Any]) -> dict[str, Any] | None:
        values = item.get("values")
        if not isinstance(values, dict):
            return None
        return {
            "kind": "trade",
            "symbol": _strip_code(item.get("item")),
            "price": _parse_price(values.get("10")),
            "change": _parse_dec(values.get("11")),
            "change_rate": _parse_dec(values.get("12")),
            "volume": int(_parse_num(values.get("13"))),
            "trade_amount": int(_parse_num(values.get("14"))),
            "exec_qty": int(_parse_num(values.get("15"))),
            "open": _parse_price(values.get("16")),
            "high": _parse_price(values.get("17")),
            "low": _parse_price(values.get("18")),
            "best_ask": _parse_price(values.get("27")),
            "best_bid": _parse_price(values.get("28")),
            "time": str(values.get("20") or ""),
            "raw": item,
        }

    def _parse_real_orderbook(self, item: dict[str, Any]) -> dict[str, Any] | None:
        values = item.get("values")
        if not isinstance(values, dict):
            return None

        asks: list[dict[str, Decimal | int]] = []
        bids: list[dict[str, Decimal | int]] = []
        for level in range(10):
            ask_price = _parse_price(values.get(str(41 + level)))
            ask_qty = int(_parse_num(values.get(str(61 + level))))
            bid_price = _parse_price(values.get(str(51 + level)))
            bid_qty = int(_parse_num(values.get(str(71 + level))))
            if ask_price != 0 or ask_qty != 0:
                asks.append({"price": ask_price, "qty": ask_qty})
            if bid_price != 0 or bid_qty != 0:
                bids.append({"price": bid_price, "qty": bid_qty})

        asks.sort(key=lambda row: row["price"])
        bids.sort(key=lambda row: row["price"], reverse=True)
        return {
            "kind": "orderbook",
            "symbol": _strip_code(item.get("item")),
            "asks": asks,
            "bids": bids,
            "best_ask": asks[0]["price"] if asks else Decimal("0"),
            "best_bid": bids[0]["price"] if bids else Decimal("0"),
            "time": str(values.get("21") or ""),
            "raw": item,
        }

    def _parse_real(self, frame: dict[str, Any]) -> list[dict[str, Any]]:
        parsed: list[dict[str, Any]] = []
        data = frame.get("data")
        if not isinstance(data, list):
            return parsed
        for item in data:
            if not isinstance(item, dict):
                continue
            stream_type = str(item.get("type") or "")
            if stream_type == "0B":
                event = self._parse_real_trade(item)
            elif stream_type == "0D":
                event = self._parse_real_orderbook(item)
            else:
                event = None
            if event is not None:
                parsed.append(event)
        return parsed

    async def _ws_login_and_register(self, ws: Any, symbols: list[str]) -> None:
        await self._ensure_access_token()
        await ws.send(json.dumps({"trnm": "LOGIN", "token": self._access_token}, ensure_ascii=False))
        login_frame = self._parse_ws_payload(await ws.recv())
        self._check_ws_response(login_frame, "LOGIN")

        registration = {
            "trnm": "REG",
            "grp_no": "1",
            "refresh": "1",
            "data": [{"item": [_strip_code(symbol) for symbol in symbols], "type": ["0B", "0D"]}],
        }
        await ws.send(json.dumps(registration, ensure_ascii=False))
        reg_frame = self._parse_ws_payload(await ws.recv())
        self._check_ws_response(reg_frame, "REG")

    async def _ws_connect_loop(self, symbols: list[str]) -> AsyncIterator[dict[str, Any]]:
        if websockets is None:
            raise RuntimeError("websockets package is not available")

        backoff_seconds = 1
        while True:
            try:
                async with websockets.connect(self.websocket_url, ping_interval=None, max_size=None) as ws:  # type: ignore[union-attr]
                    backoff_seconds = 1
                    await self._ws_login_and_register(ws, symbols)
                    while True:
                        raw = await ws.recv()
                        frame = self._parse_ws_payload(raw)
                        if frame is None:
                            continue
                        trnm = str(frame.get("trnm") or "")
                        if trnm == "PING":
                            await ws.send(raw)
                            continue
                        if trnm == "REAL":
                            for event in self._parse_real(frame):
                                yield event
                            continue
                        if "return_code" in frame and str(frame.get("return_code")) not in {"0", ""}:
                            raise KiwoomApiError(str(frame.get("return_code")), str(frame.get("return_msg") or "websocket error"), frame)
            except KiwoomApiError:
                raise
            except Exception:
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterator[dict[str, Any]]:
        async for event in self._ws_connect_loop(symbols):
            yield event

    async def stream_market_ticks(self, symbols: list[str]) -> AsyncIterator[dict[str, Any]]:
        async for quote in self.stream_quotes(symbols):
            if quote.get("kind") == "trade":
                yield quote

    async def stream_fills(self, account_id: str | None = None) -> AsyncIterator[dict[str, Any]]:
        _ = account_id
        # TODO(openapi.kiwoom.com): implement order/fill notification streams after
        # measuring Kiwoom account execution types "00"/"04". Do not use stock
        # trade type "0B" here; it is market trade print data, not account fills.
        raise NotImplementedError("Kiwoom account fill websocket types 00/04 are not measured yet")
        yield {}


BrokerAdapter = KiwoomRestKrBaseAdapter
