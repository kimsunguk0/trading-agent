"""Decision engine: SignalEvent -> OrderIntentEvent."""

from __future__ import annotations

import inspect
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from core.events.bus import RedisStreamBus
from core.events.schemas import ORDER_INTENTS_STREAM, EventType, OrderIntentEvent, RiskEvent, SignalEvent
from core.models.market import Side
from core.models.order import OrderRequest, RiskCheckResult
from core.models.portfolio import Account
from core.risk.gate import RiskGate
from core.trading_controls import is_entry_allowed


logger = logging.getLogger(__name__)


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _to_account(snapshot: Any) -> Account:
    if isinstance(snapshot, Account):
        return snapshot
    if hasattr(snapshot, "account_id"):
        return Account(
            account_id=str(snapshot.account_id),
            cash_balance=_to_decimal(getattr(snapshot, "cash_balance", "0")),
            currency=str(getattr(snapshot, "currency", "KRW")),
        )
    return Account(account_id="default", cash_balance=Decimal("0"))


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _price_from_level(value: Any) -> Decimal | None:
    if isinstance(value, dict):
        value = value.get("price")
    if value in (None, ""):
        return None
    price = _to_decimal(value)
    if price <= Decimal("0"):
        return None
    return price


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


@dataclass
class DecisionPolicyEngine:
    account_id: str
    date_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    _seq: dict[tuple[str, str, str], int] = field(default_factory=dict)

    def next_order_intent_id(self, market: str, symbol: str) -> str:
        current = self.date_fn().strftime("%Y%m%d")
        key = (current, market, symbol)
        seq = self._seq.get(key, 0) + 1
        self._seq[key] = seq
        return f"OI-{current}-{market}-{symbol}-{seq:04d}"


class DecisionEngine:
    def __init__(
        self,
        broker: Any,
        bus: RedisStreamBus,
        risk_gate: RiskGate | None = None,
        account_id: str = "default",
    ) -> None:
        self.broker = broker
        self.bus = bus
        self.risk_gate = risk_gate or RiskGate()
        self.account_id = account_id
        self.policy = DecisionPolicyEngine(account_id=account_id)
        self._positions: dict[tuple[str, str], Decimal] = defaultdict(Decimal)

    async def _account(self) -> Account | None:
        if hasattr(self.broker, "get_account"):
            accessor = getattr(self.broker, "get_account")
            if callable(accessor):
                value = await _maybe_await(accessor(self.account_id))
                return _to_account(value)
        for method_name in ("get_cash", "get_cash_snapshot"):
            accessor = getattr(self.broker, method_name, None)
            if callable(accessor):
                snapshot = await _maybe_await(accessor(self.account_id))
                return _to_account(snapshot)
        return None

    def _position_key(self, account_id: str, symbol: str) -> tuple[str, str]:
        return (account_id, symbol)

    def _can_send(self, signal: SignalEvent) -> bool:
        key = self._position_key(signal.account_id, signal.symbol.value)

        if signal.side == Side.BUY:
            return self._positions[key] <= Decimal("0")

        if signal.side == Side.SELL:
            return self._positions[key] > Decimal("0")

        return False

    async def _risk_check(self, request: OrderRequest) -> RiskCheckResult:
        account = await self._account()
        return await self.risk_gate.evaluate(request, account)

    def _qty_from_signal(self, signal: SignalEvent) -> Decimal:
        raw = signal.payload.get("quantity")
        if raw is None:
            return Decimal("1")
        return _to_decimal(raw)

    def _market_from_signal(self, signal: SignalEvent) -> str:
        payload_market = signal.payload.get("market")
        if isinstance(payload_market, str):
            return payload_market
        symbol = signal.symbol.value
        return "KR" if symbol.isdigit() else "US"

    def _execution_payload(self, signal: SignalEvent) -> dict[str, Any]:
        execution = signal.payload.get("execution")
        return execution if isinstance(execution, dict) else {}

    def _order_type_from_signal(self, signal: SignalEvent) -> str:
        execution = self._execution_payload(signal)
        raw = signal.payload.get("order_type") or execution.get("order_type") or "MARKET"
        return str(raw).upper()

    def _allow_market_order(self, signal: SignalEvent) -> bool:
        execution = self._execution_payload(signal)
        if "allow_market_order" in signal.payload:
            return _to_bool(signal.payload.get("allow_market_order"))
        if "allow_market_order" in execution:
            return _to_bool(execution.get("allow_market_order"))
        return False

    def _limit_price_basis(self, signal: SignalEvent) -> str:
        execution = self._execution_payload(signal)
        raw = signal.payload.get("limit_price_basis") or execution.get("limit_price_basis")
        if isinstance(raw, str) and raw.strip():
            return raw.strip().lower()
        return "best_bid" if signal.side == Side.SELL else "best_ask"

    def _signal_price(self, signal: SignalEvent) -> Decimal | None:
        price = signal.payload.get("price")
        if price in (None, ""):
            return None
        resolved = _to_decimal(price)
        if resolved <= Decimal("0"):
            return None
        return resolved

    async def _call_market_data(self, method_name: str, symbol: str) -> dict[str, Any] | None:
        accessor = getattr(self.broker, method_name, None)
        if not callable(accessor):
            return None
        try:
            payload = await _maybe_await(accessor(symbol))
        except Exception:
            logger.warning("Failed to fetch market data for limit price.", extra={"method": method_name, "symbol": symbol}, exc_info=True)
            return None
        return payload if isinstance(payload, dict) else None

    def _price_from_orderbook(self, orderbook: dict[str, Any], basis: str, side: Side) -> Decimal | None:
        ask = (
            _price_from_level(orderbook.get("best_ask"))
            or _price_from_level(orderbook.get("ask"))
            or _price_from_level((orderbook.get("asks") or [None])[0] if isinstance(orderbook.get("asks"), list) else None)
        )
        bid = (
            _price_from_level(orderbook.get("best_bid"))
            or _price_from_level(orderbook.get("bid"))
            or _price_from_level((orderbook.get("bids") or [None])[0] if isinstance(orderbook.get("bids"), list) else None)
        )
        if basis in {"best_ask", "ask"}:
            return ask
        if basis in {"best_bid", "bid"}:
            return bid
        if basis == "mid" and ask is not None and bid is not None:
            return (ask + bid) / Decimal("2")
        return ask if side == Side.BUY else bid

    def _price_from_quote(self, quote: dict[str, Any], basis: str, side: Side) -> Decimal | None:
        ask = _price_from_level(quote.get("best_ask")) or _price_from_level(quote.get("ask"))
        bid = _price_from_level(quote.get("best_bid")) or _price_from_level(quote.get("bid"))
        last = _price_from_level(quote.get("price")) or _price_from_level(quote.get("last"))
        if basis in {"best_ask", "ask"}:
            return ask or last
        if basis in {"best_bid", "bid"}:
            return bid or last
        if basis == "mid" and ask is not None and bid is not None:
            return (ask + bid) / Decimal("2")
        if basis in {"last", "price"}:
            return last
        if side == Side.BUY:
            return ask or last
        return bid or last

    async def _resolve_limit_price(self, signal: SignalEvent) -> Decimal | None:
        signal_price = self._signal_price(signal)
        if signal_price is not None:
            return signal_price

        symbol = signal.symbol.value
        basis = self._limit_price_basis(signal)
        orderbook = await self._call_market_data("get_orderbook", symbol)
        if orderbook is not None:
            price = self._price_from_orderbook(orderbook, basis, signal.side)
            if price is not None:
                return price

        quote = await self._call_market_data("get_quote", symbol)
        if quote is not None:
            return self._price_from_quote(quote, basis, signal.side)

        return None

    async def _resolve_order_pricing(self, signal: SignalEvent) -> tuple[str, Decimal | None] | None:
        order_type = self._order_type_from_signal(signal)
        if order_type != "LIMIT":
            return order_type, None

        price = await self._resolve_limit_price(signal)
        if price is not None:
            return order_type, price

        if self._allow_market_order(signal):
            logger.warning(
                "Falling back to MARKET because limit price could not be resolved.",
                extra={"symbol": signal.symbol.value, "strategy_id": signal.strategy_id},
            )
            return "MARKET", None

        logger.warning(
            "Skipping signal because LIMIT order has no resolvable price.",
            extra={"symbol": signal.symbol.value, "strategy_id": signal.strategy_id},
        )
        return None

    async def _publish_order_intent(self, event: OrderIntentEvent) -> None:
        payload = event.model_dump(mode="json", by_alias=True)
        body = json.dumps(payload, ensure_ascii=False)

        # Canonical executable order-intent stream. Do not also publish through
        # bus.publish(event), which maps EventType.ORDER_INTENT to singular
        # `order_intent` and can create duplicate downstream side effects.
        stream = self.bus.stream_name(ORDER_INTENTS_STREAM)
        if self.bus._client is None:
            self.bus._fallback_streams[stream].append(body)
            for queue in self.bus._fallback_subscribers.get(stream, []):
                queue.put_nowait(body)
            return

        await self.bus._client.xadd(stream, {"payload": body})

    async def process_signal(self, signal: SignalEvent) -> OrderIntentEvent | None:
        if not self._can_send(signal):
            return None

        quantity = self._qty_from_signal(signal)
        pricing = await self._resolve_order_pricing(signal)
        if pricing is None:
            return None
        order_type, price = pricing

        market = self._market_from_signal(signal)
        if signal.side == Side.BUY:
            environment = os.getenv("ENVIRONMENT", "paper").lower()
            allowed, reason = await is_entry_allowed(
                signal.symbol.value,
                market,
                environment=environment,
                dsn=os.getenv("DATABASE_URL"),
                redis_url=getattr(self.bus, "redis_url", os.getenv("REDIS_URL", "")),
                stream_prefix=getattr(self.bus, "stream_prefix", os.getenv("REDIS_STREAM_PREFIX", f"{environment}.events")),
            )
            if not allowed:
                await self.bus.publish(
                    RiskEvent(
                        event_type=EventType.RISK,
                        order_intent_id=f"CONTROL-{market}-{signal.symbol.value}",
                        stage="trading_control",
                        passed=False,
                        reason=reason,
                    )
                )
                return None

        order_intent_id = self.policy.next_order_intent_id(market, signal.symbol.value)

        request = OrderRequest(
            order_intent_id=order_intent_id,
            account_id=signal.account_id,
            symbol=signal.symbol.value,
            side=signal.side.value,
            quantity=quantity,
            price=price,
            order_type=order_type,
        )

        risk_result = await self._risk_check(request)
        if not risk_result.passed:
            await self.bus.publish(
                RiskEvent(
                    event_type=EventType.RISK,
                    order_intent_id=request.order_intent_id,
                    stage="decision",
                    passed=False,
                    reason=risk_result.reason,
                )
            )
            return None

        event = OrderIntentEvent(
            event_type=EventType.ORDER_INTENT,
            request=request,
        )

        key = self._position_key(signal.account_id, signal.symbol.value)
        if signal.side == Side.BUY:
            self._positions[key] += request.quantity
        else:
            self._positions[key] -= request.quantity
            if self._positions[key] <= Decimal("0"):
                self._positions.pop(key, None)

        await self._publish_order_intent(event)
        return event

    async def run(self) -> None:
        async for event in self.bus.subscribe("signals"):
            if not isinstance(event, SignalEvent):
                continue
            await self.process_signal(event)


async def main() -> None:
    import os

    from brokers.kis_domestic_kr_mock import KISDomesticKrMockAdapter
    from brokers.simulated import SimulatedBrokerAdapter
    from brokers.kiwoom_rest_kr_mock import KiwoomRestKrMockAdapter

    adapter_name = os.getenv("BROKER_ADAPTER", "simulated").lower()
    if adapter_name == "kiwoom_mock":
        broker = KiwoomRestKrMockAdapter()
    elif adapter_name in {"kis_kr_mock", "kis_domestic_mock", "kis_domestic_kr_mock"}:
        broker = KISDomesticKrMockAdapter()
    else:
        broker = SimulatedBrokerAdapter()

    bus = RedisStreamBus(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        stream_prefix=os.getenv("REDIS_STREAM_PREFIX", "paper.events"),
    )

    await DecisionEngine(
        broker=broker,
        bus=bus,
        account_id=os.getenv("ACCOUNT_ID", "default"),
    ).run()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
