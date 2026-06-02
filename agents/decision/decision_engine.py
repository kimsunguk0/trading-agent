"""Decision engine: SignalEvent -> OrderIntentEvent."""

from __future__ import annotations

import inspect
import json
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable

from core.events.bus import RedisStreamBus
from core.events.schemas import EventType, OrderIntentEvent, RiskEvent, SignalEvent
from core.models.market import Side
from core.models.order import OrderRequest, RiskCheckResult
from core.models.portfolio import Account
from core.risk.gate import RiskGate
from core.trading_controls import is_entry_allowed


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
                if inspect.iscoroutinefunction(accessor):
                    return _to_account(await accessor(self.account_id))
                value = accessor(self.account_id)
                return _to_account(value)
        if hasattr(self.broker, "get_cash_snapshot"):
            accessor = getattr(self.broker, "get_cash_snapshot")
            if inspect.iscoroutinefunction(accessor):
                snapshot = await accessor(self.account_id)
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

    async def _publish_order_intent(self, event: OrderIntentEvent) -> None:
        payload = event.model_dump(mode="json", by_alias=True)
        body = json.dumps(payload, ensure_ascii=False)

        await self.bus.publish(event)

        # Execution worker currently subscribes to `order_intents` (plural).
        stream = self.bus.stream_name("order_intents")
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
        price = signal.payload.get("price")
        order_type = str(signal.payload.get("order_type", "MARKET"))

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
            price=_to_decimal(price) if price is not None else None,
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

    from brokers.simulated import SimulatedBrokerAdapter
    from brokers.kiwoom_rest_kr_mock import KiwoomRestKrMockAdapter

    adapter_name = os.getenv("BROKER_ADAPTER", "simulated").lower()
    if adapter_name == "kiwoom_mock":
        broker = KiwoomRestKrMockAdapter()
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
