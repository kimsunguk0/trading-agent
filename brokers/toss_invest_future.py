"""Toss Securities Open API adapter.

Toss Invest has no sandbox endpoint as of the confirmed 2026 Open API spec.
This adapter is therefore live-only: orders sent through it may use real cash.
Request contracts confirmed by the user-provided official guide are implemented
directly. Response schemas that are not yet measured are parsed defensively and
marked with TODO comments.
"""

from __future__ import annotations

import asyncio
import base64
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, AsyncIterator

import httpx

from brokers.base import BrokerCapabilities
from brokers.capabilities import TOSS_CAPS
from core.models.market import Symbol
from core.models.order import OrderAck, OrderRequest, OrderStatus
from core.models.portfolio import Account, CashSnapshot, Position


TOSS_BASE_URL = "https://openapi.tossinvest.com"


class TossApiError(RuntimeError):
    """Raised when Toss Invest Open API returns an unusable payload."""

    def __init__(self, code: str, message: str, payload: dict[str, Any] | None = None) -> None:
        super().__init__(f"Toss Invest API error {code}: {message}")
        self.code = code
        self.message = message
        self.payload = payload or {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dec(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    text = str(value).strip().replace(",", "")
    if text == "":
        return Decimal("0")
    return Decimal(text)


def _parse_int(value: Any) -> int:
    return int(_parse_dec(value))


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


def _body(payload: dict[str, Any]) -> dict[str, Any]:
    for key in ("data", "result", "output", "body"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return payload


def _amount_value(value: Any, currency: str = "krw") -> Decimal:
    if isinstance(value, dict):
        for key in (currency, currency.upper(), "amount", "value"):
            if value.get(key) not in (None, ""):
                return _parse_dec(value.get(key))
        return Decimal("0")
    return _parse_dec(value)


def _client_order_id(request: OrderRequest) -> str:
    extra = getattr(request, "model_extra", None)
    if not isinstance(extra, dict):
        extra = {}
    for key in ("clientOrderId", "client_order_id", "idempotency_key"):
        value = extra.get(key) or getattr(request, key, None)
        if value not in (None, ""):
            return str(value)
    return request.order_intent_id


def _as_list(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _as_list(value, *keys)
            if nested:
                return nested
    for key in ("data", "result", "output", "body"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _as_list(value, *keys)
            if nested:
                return nested
    return []


def _strip_code(value: Any) -> str:
    return str(value or "").strip().upper()


def _basic_auth_header(app_key: str, app_secret: str) -> str:
    raw = f"{app_key}:{app_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _status_from_payload(payload: dict[str, Any]) -> OrderStatus:
    # TODO(developers.tossinvest.com): confirm exact order status enum values.
    raw = str(_first(payload, "status", "orderStatus", "state", "orderState", default="")).upper()
    if raw in {"FILLED", "COMPLETE", "COMPLETED", "DONE", "체결"}:
        return OrderStatus.FILLED
    if raw in {"PARTIALLY_FILLED", "PARTIAL", "PARTIAL_FILLED", "부분체결"}:
        return OrderStatus.PARTIALLY_FILLED
    if raw in {"REJECTED", "FAILED", "ERROR", "거부"}:
        return OrderStatus.REJECTED
    if raw in {"NOT_FOUND", "NOTFOUND"}:
        return OrderStatus.NOT_FOUND
    if raw in {"ACKED", "ACCEPTED", "BROKER_ACKED", "접수"}:
        return OrderStatus.BROKER_ACKED
    return OrderStatus.SUBMITTED


class TossInvestAdapter:
    """Live-only Toss Securities Open API adapter."""

    name: str = "toss_invest"
    capabilities: BrokerCapabilities = TOSS_CAPS
    default_base_url: str = TOSS_BASE_URL

    def __init__(
        self,
        app_key: str | None = None,
        app_secret: str | None = None,
        account_no: str | None = None,
        base_url: str | None = None,
        rate_limit: int = 5,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.app_key = app_key or os.getenv("TOSS_APP_KEY", "")
        self.app_secret = app_secret or os.getenv("TOSS_APP_SECRET", "")
        self.account_no = account_no or os.getenv("TOSS_ACCOUNT_NO", "")
        if not self.app_key or not self.app_secret:
            raise ValueError("TOSS_APP_KEY/TOSS_APP_SECRET are required")
        if not self.account_no:
            raise ValueError("TOSS_ACCOUNT_NO is required")

        self.base_url = (base_url or os.getenv("TOSS_BASE_URL") or self.default_base_url).rstrip("/")
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0, transport=transport)
        self._rate_limit = max(1, int(rate_limit))
        self._request_times: list[float] = []
        self._access_token: str | None = None
        self._access_token_expires_at: datetime | None = None
        self._order_mapping: dict[str, str] = {}
        self._last_order_ack: dict[str, OrderAck] = {}

    def _now(self) -> datetime:
        return _now()

    async def close(self) -> None:
        await self._client.aclose()

    async def _rate_limit_wait(self) -> None:
        loop = asyncio.get_event_loop()
        now = loop.time()
        self._request_times = [item for item in self._request_times if now - item < 1.0]
        if len(self._request_times) >= self._rate_limit:
            await asyncio.sleep(1.0 - (now - self._request_times[0]))
            now = loop.time()
            self._request_times = [item for item in self._request_times if now - item < 1.0]
        self._request_times.append(loop.time())

    async def _needs_refresh(self) -> bool:
        if self._access_token is None or self._access_token_expires_at is None:
            return True
        return self._now() >= (self._access_token_expires_at - timedelta(minutes=1))

    async def _request_access_token(self) -> None:
        await self._rate_limit_wait()
        response = await self._client.post(
            "/oauth2/token",
            data={"grant_type": "client_credentials"},
            headers={
                "Authorization": _basic_auth_header(self.app_key, self.app_secret),
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise TossApiError("-1", "invalid token response payload", {"raw": str(payload)})
        token = payload.get("access_token")
        if not token:
            raise TossApiError("-1", "token response does not include access_token", payload)
        expires_in = int(_parse_dec(payload.get("expires_in") or "3600"))
        self._access_token = str(token)
        self._access_token_expires_at = self._now() + timedelta(seconds=max(60, expires_in))

    async def _ensure_access_token(self) -> None:
        if await self._needs_refresh():
            await self._request_access_token()

    def _headers(self, *, account: bool = False, content_type: bool = False) -> dict[str, str]:
        if not self._access_token:
            raise TossApiError("-1", "access token is not initialized")
        headers = {"Authorization": f"Bearer {self._access_token}"}
        if account:
            headers["X-Tossinvest-Account"] = self.account_no
        if content_type:
            headers["Content-Type"] = "application/json"
        return headers

    async def _get(self, path: str, *, params: dict[str, Any] | None = None, account: bool = False) -> dict[str, Any] | list[Any]:
        await self._rate_limit_wait()
        await self._ensure_access_token()
        response = await self._client.get(path, params=params or {}, headers=self._headers(account=account))
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, (dict, list)):
            raise TossApiError("-1", "invalid response payload", {"raw": str(payload)})
        return payload

    async def _post(self, path: str, body: dict[str, Any], *, account: bool = False) -> dict[str, Any]:
        await self._rate_limit_wait()
        await self._ensure_access_token()
        response = await self._client.post(
            path,
            json=body,
            headers=self._headers(account=account, content_type=True),
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise TossApiError("-1", "invalid response payload", {"raw": str(payload)})
        return payload

    async def _patch(self, path: str, body: dict[str, Any], *, account: bool = False) -> dict[str, Any]:
        await self._rate_limit_wait()
        await self._ensure_access_token()
        response = await self._client.patch(
            path,
            json=body,
            headers=self._headers(account=account, content_type=True),
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {"raw": payload}

    async def _delete(self, path: str, *, account: bool = False) -> dict[str, Any]:
        await self._rate_limit_wait()
        await self._ensure_access_token()
        response = await self._client.delete(path, headers=self._headers(account=account))
        response.raise_for_status()
        if not response.content:
            return {}
        payload = response.json()
        return payload if isinstance(payload, dict) else {"raw": payload}

    async def get_accounts(self) -> list[str]:
        payload = await self._get("/api/v1/accounts", account=False)
        rows = _as_list(payload, "accounts", "accountList", "items")
        accounts = [
            str(_first(row, "accountNo", "accountNumber", "account_id", "accountId", "id", default="")).strip()
            for row in rows
        ]
        accounts = [item for item in accounts if item]
        return accounts or [self.account_no]

    async def get_cash(self, account_id: str | None = None) -> CashSnapshot:
        _ = account_id
        # TODO(developers.tossinvest.com): official latest OpenAPI JSON did not
        # expose a cash/buying-power endpoint. Do not call guessed paths because
        # Toss is live-only and risk checks must not rely on invented balances.
        raise NotImplementedError("Toss cash balance endpoint is not confirmed in the official OpenAPI spec")

    async def get_cash_snapshot(self, account_id: str) -> CashSnapshot:
        return await self.get_cash(account_id)

    async def get_positions(self, account_id: str | None = None) -> dict[str, dict[str, Any]]:
        _ = account_id
        payload = await self._get("/api/v1/holdings", account=True)
        if not isinstance(payload, dict):
            raise TossApiError("-1", "invalid holdings response payload", {"raw": str(payload)})
        rows = _as_list(payload, "items")
        positions: dict[str, dict[str, Any]] = {}
        for row in rows:
            symbol = _strip_code(_first(row, "symbol", "stockCode", "stock_code", "code", "isuCode", default=""))
            if not symbol:
                continue
            qty = _parse_dec(_first(row, "quantity", "qty", default="0"))
            if qty == 0:
                continue
            market_value = row.get("marketValue") if isinstance(row.get("marketValue"), dict) else {}
            profit_loss = row.get("profitLoss") if isinstance(row.get("profitLoss"), dict) else {}
            positions[symbol] = {
                "symbol": symbol,
                "name": str(_first(row, "name", "stockName", "stock_name", "symbolName", default="")),
                "quantity": qty,
                "tradeable_quantity": qty,
                "average_price": _amount_value(_first(row, "averagePurchasePrice", "averagePrice", "avgPrice", default="0")),
                "current_price": _amount_value(_first(row, "lastPrice", "currentPrice", "marketPrice", default="0")),
                "unrealized_pnl": _amount_value(_first(profit_loss, "amount", "amountAfterCost", default="0")),
                "profit_rate": _parse_dec(_first(profit_loss, "rate", "rateAfterCost", default="0")),
                "evaluation_amount": _amount_value(_first(market_value, "amount", "amountAfterCost", default="0")),
                "purchase_amount": _amount_value(_first(market_value, "purchaseAmount", default="0")),
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
                quantity=_parse_dec(row.get("quantity")),
                average_price=_parse_dec(row.get("average_price")),
                realized_pnl=_parse_dec(row.get("unrealized_pnl")),
            )
            for symbol, row in raw_positions.items()
        }
        return Account(account_id=account_id, cash_balance=cash.cash_balance, positions=positions)

    async def get_quote(self, symbol: str | Symbol) -> dict[str, Any]:
        code = _strip_code(symbol)
        # TODO(developers.tossinvest.com): confirm whether /api/v1/prices uses
        # "symbol" or "symbols" as the query parameter in all deployments.
        try:
            payload = await self._get("/api/v1/prices", params={"symbol": code})
        except httpx.HTTPStatusError:
            payload = await self._get("/api/v1/prices", params={"symbols": code})
        rows = _as_list(payload)
        body = next((row for row in rows if _strip_code(row.get("symbol")) == code), rows[0] if rows else {})
        if not body:
            payload = await self._get("/api/v1/prices", params={"symbols": code})
            rows = _as_list(payload)
            body = next((row for row in rows if _strip_code(row.get("symbol")) == code), rows[0] if rows else {})
        return {
            "symbol": _strip_code(_first(body, "symbol", "stockCode", "stock_code", "code", default=code)),
            "name": str(_first(body, "name", "stockName", "stock_name", default="")),
            "market": str(_first(body, "market", "marketCode", default="KR")),
            "price": _amount_value(_first(body, "lastPrice", "currentPrice", "tradePrice", "price", default="0")),
            "prev_close": _amount_value(_first(body, "previousClosePrice", "prevClose", "prev_close", "basePrice", default="0")),
            "change": _parse_dec(_first(body, "change", "changePrice", "priceChange", default="0")),
            "change_rate": _parse_dec(_first(body, "changeRate", "fluctuationRate", "rate", default="0")),
            "volume": _parse_int(_first(body, "volume", "accumulatedVolume", "tradingVolume", default="0")),
            "is_halted": str(_first(body, "isHalted", "halted", "tradingHalt", default="false")).lower() in {"1", "true", "y", "yes"},
            "currency": str(_first(body, "currency", default="")),
            "occurred_at": str(_first(body, "timestamp", default=self._now().isoformat())),
            "raw": payload,
        }

    async def get_market_tick(self, symbol: str | Symbol) -> dict[str, Any]:
        return await self.get_quote(symbol)

    def _parse_orderbook_levels(self, body: dict[str, Any], side: str) -> list[dict[str, Decimal | int]]:
        if side == "ask":
            list_keys = ("asks", "ask", "sell", "sellQuotes", "sellOrderbook", "askLevels")
            price_keys = ("price", "askPrice", "sellPrice", "orderPrice")
            qty_keys = ("volume", "quantity", "qty", "askQuantity", "sellQuantity", "orderQuantity")
            indexed_price = ("askPrice{n}", "ask{n}Price", "sellPrice{n}", "ask_{n}_price")
            indexed_qty = ("askQuantity{n}", "ask{n}Quantity", "sellQuantity{n}", "ask_{n}_quantity")
        else:
            list_keys = ("bids", "bid", "buy", "buyQuotes", "buyOrderbook", "bidLevels")
            price_keys = ("price", "bidPrice", "buyPrice", "orderPrice")
            qty_keys = ("volume", "quantity", "qty", "bidQuantity", "buyQuantity", "orderQuantity")
            indexed_price = ("bidPrice{n}", "bid{n}Price", "buyPrice{n}", "bid_{n}_price")
            indexed_qty = ("bidQuantity{n}", "bid{n}Quantity", "buyQuantity{n}", "bid_{n}_quantity")

        rows = _as_list(body, *list_keys)
        levels = []
        for row in rows:
            price = _parse_dec(_first(row, *price_keys, default="0"))
            qty = _parse_int(_first(row, *qty_keys, default="0"))
            if price != 0 or qty != 0:
                levels.append({"price": price, "quantity": qty})

        if levels:
            return levels

        for level in range(1, 11):
            price = _parse_dec(_first(body, *(pattern.format(n=level) for pattern in indexed_price), default="0"))
            qty = _parse_int(_first(body, *(pattern.format(n=level) for pattern in indexed_qty), default="0"))
            if price != 0 or qty != 0:
                levels.append({"price": price, "quantity": qty})
        return levels

    async def get_orderbook(self, symbol: str | Symbol) -> dict[str, Any]:
        code = _strip_code(symbol)
        payload = await self._get("/api/v1/orderbook", params={"symbol": code})
        if not isinstance(payload, dict):
            raise TossApiError("-1", "invalid orderbook response payload", {"raw": str(payload)})
        body = _body(payload)
        asks = self._parse_orderbook_levels(body, "ask")
        bids = self._parse_orderbook_levels(body, "bid")
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
            "total_ask_qty": _parse_int(_first(body, "totalAskQuantity", "total_ask_qty", "totAskQty", default="0")),
            "total_bid_qty": _parse_int(_first(body, "totalBidQuantity", "total_bid_qty", "totBidQty", default="0")),
            "raw": payload,
        }

    def _order_body(self, request: OrderRequest) -> dict[str, Any]:
        side = request.side.upper()
        order_type = str(request.order_type or "").upper()
        extra = getattr(request, "model_extra", None)
        if not isinstance(extra, dict):
            extra = {}
        body = {
            "symbol": _strip_code(request.symbol),
            "side": side,
            "quantity": _decimal_string(request.quantity),
            "orderType": order_type,
            "clientOrderId": _client_order_id(request),
        }
        if request.price is not None:
            body["price"] = _decimal_string(request.price)
        time_in_force = extra.get("timeInForce") or extra.get("time_in_force") or getattr(request, "time_in_force", None)
        if time_in_force:
            # TODO(developers.tossinvest.com): confirm runtime support matrix for
            # DAY vs CLS by market/order type.
            body["timeInForce"] = str(time_in_force).upper()
        order_amount = extra.get("orderAmount") or extra.get("order_amount") or getattr(request, "order_amount", None)
        if order_amount is not None:
            body["orderAmount"] = _decimal_string(order_amount)
        return body

    async def submit_order(self, request: OrderRequest) -> OrderAck:
        response = await self._post("/api/v1/orders", self._order_body(request), account=True)
        body = _body(response)
        broker_order_id = str(_first(body, "orderId", "order_id", "id", "orderNo", "orderNumber", default="")).strip()
        if not broker_order_id:
            raise TossApiError("-1", "order response does not include order id", response)

        ack = OrderAck(
            order_id=broker_order_id,
            order_intent_id=request.order_intent_id,
            status=_status_from_payload(body),
            filled_quantity=_parse_dec(_first(body, "filledQuantity", "filled_qty", "executedQuantity", default="0")),
            total_quantity=_parse_dec(_first(body, "quantity", "orderQuantity", "totalQuantity", default=request.quantity)),
            average_fill_price=(
                _parse_dec(_first(body, "averageFillPrice", "avgFillPrice", "executedPrice", default=request.price))
                if _first(body, "averageFillPrice", "avgFillPrice", "executedPrice", default=request.price) is not None
                else None
            ),
        )
        self._order_mapping[request.order_intent_id] = broker_order_id
        self._order_mapping[_client_order_id(request)] = broker_order_id
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
        _ = account_id, quantity
        response = await self._delete(f"/api/v1/orders/{broker_order_id}", account=True)
        return {"order_id": broker_order_id, "result": response}

    async def modify_order(
        self,
        account_id: str,
        broker_order_id: str,
        *,
        quantity: Decimal | int | str | None = None,
        price: Decimal | int | str | None = None,
    ) -> dict[str, Any]:
        _ = account_id
        # TODO(developers.tossinvest.com): confirm exact modify-order JSON property names.
        body: dict[str, Any] = {}
        if quantity is not None:
            body["quantity"] = _decimal_string(quantity)
        if price is not None:
            body["price"] = _decimal_string(price)
        response = await self._patch(f"/api/v1/orders/{broker_order_id}", body, account=True)
        return {"order_id": broker_order_id, "result": response}

    async def get_order_status(self, order_intent_id: str) -> OrderAck | None:
        broker_order_id = self._order_mapping.get(order_intent_id, order_intent_id)
        # TODO(developers.tossinvest.com): confirm whether status lookup accepts
        # only orderId or also clientOrderId.
        response = await self._get(f"/api/v1/orders/{broker_order_id}", account=True)
        body = _body(response)
        order_id = str(_first(body, "orderId", "order_id", "id", "orderNo", default=broker_order_id))
        quantity = _parse_dec(_first(body, "quantity", "orderQuantity", "totalQuantity", default="0"))
        ack = OrderAck(
            order_id=order_id,
            order_intent_id=order_intent_id,
            status=_status_from_payload(body),
            filled_quantity=_parse_dec(_first(body, "filledQuantity", "filled_qty", "executedQuantity", default="0")),
            total_quantity=quantity,
            average_fill_price=(
                _parse_dec(_first(body, "averageFillPrice", "avgFillPrice", "executedPrice"))
                if _first(body, "averageFillPrice", "avgFillPrice", "executedPrice") is not None
                else None
            ),
        )
        self._last_order_ack[order_intent_id] = ack
        return ack

    async def is_tradable(self, symbol: str | Symbol) -> bool:
        quote = await self.get_quote(symbol)
        if bool(quote.get("is_halted")):
            return False
        return _parse_dec(quote.get("price")) > Decimal("0")

    async def stream_quotes(self, symbols: list[str]) -> AsyncIterator[dict[str, Any]]:
        interval = max(1.0, float(os.getenv("TOSS_QUOTE_POLL_INTERVAL_SECONDS", "1")))
        while True:
            for symbol in symbols:
                yield await self.get_quote(symbol)
            await asyncio.sleep(interval)

    async def stream_market_ticks(self, symbols: list[str]) -> AsyncIterator[dict[str, Any]]:
        async for quote in self.stream_quotes(symbols):
            yield quote

    async def stream_fills(self, account_id: str | None = None) -> AsyncIterator[dict[str, Any]]:
        _ = account_id
        # TODO(developers.tossinvest.com): implement account fill stream if Toss
        # publishes an official WebSocket/FCM contract. Do not infer fills from
        # market quote polling.
        raise NotImplementedError("Toss Invest fill streaming is not officially documented")
        yield {}


BrokerAdapter = TossInvestAdapter

__all__ = [
    "TOSS_BASE_URL",
    "TOSS_CAPS",
    "TossApiError",
    "TossInvestAdapter",
    "_basic_auth_header",
    "_parse_dec",
]
