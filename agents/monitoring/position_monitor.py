"""Deterministic stop-loss and take-profit monitor for open positions."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg

from core.events.bus import RedisStreamBus
from core.events.schemas import EventType, OrderIntentEvent, RiskEvent
from core.models.order import OrderRequest


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _as_fraction(value: Decimal) -> Decimal:
    if value > Decimal("1"):
        return value / Decimal("100")
    return value


@dataclass(frozen=True)
class PositionExitRule:
    stop_loss_pct: Decimal
    take_profit_pct: Decimal

    @classmethod
    def from_env(cls) -> "PositionExitRule":
        return cls(
            stop_loss_pct=_as_fraction(_to_decimal(os.getenv("POSITION_STOP_LOSS_PCT", "0.03"))),
            take_profit_pct=_as_fraction(_to_decimal(os.getenv("POSITION_TAKE_PROFIT_PCT", "0.06"))),
        )


@dataclass(frozen=True)
class MonitoredPosition:
    account_id: str
    symbol: str
    quantity: Decimal
    average_price: Decimal


@dataclass(frozen=True)
class PositionExitDecision:
    order_intent_id: str
    request: OrderRequest
    reason: str
    trigger_price: Decimal
    current_price: Decimal
    pnl_pct: Decimal


class PositionMonitor:
    """Monitor open positions without depending on decision or strategy workers."""

    def __init__(
        self,
        *,
        schema: str,
        account_id: str,
        environment: str = "paper",
        dsn: str | None = None,
        redis_url: str = "redis://localhost:6379/0",
        bus: RedisStreamBus | None = None,
        broker: object | None = None,
        rule: PositionExitRule | None = None,
    ) -> None:
        self.schema = schema
        self.account_id = account_id
        self.environment = environment
        self.dsn = dsn
        self.broker = broker
        self.rule = rule or PositionExitRule.from_env()
        self.bus = bus or RedisStreamBus(redis_url=redis_url, stream_prefix=f"{environment}.events")
        self._emitted: set[str] = set()

    def evaluate(
        self,
        position: MonitoredPosition,
        current_price: Decimal,
        *,
        now: datetime | None = None,
    ) -> PositionExitDecision | None:
        if position.quantity <= Decimal("0") or position.average_price <= Decimal("0") or current_price <= Decimal("0"):
            return None

        now = now or datetime.now(timezone.utc)
        pnl_pct = (current_price - position.average_price) / position.average_price
        stop_price = position.average_price * (Decimal("1") - self.rule.stop_loss_pct)
        take_price = position.average_price * (Decimal("1") + self.rule.take_profit_pct)

        reason = ""
        trigger = Decimal("0")
        if self.rule.stop_loss_pct > Decimal("0") and current_price <= stop_price:
            reason = "STOP_LOSS"
            trigger = stop_price
        elif self.rule.take_profit_pct > Decimal("0") and current_price >= take_price:
            reason = "TAKE_PROFIT"
            trigger = take_price
        else:
            return None

        order_intent_id = self._order_intent_id(position.symbol, reason, now)
        request = OrderRequest(
            order_intent_id=order_intent_id,
            account_id=position.account_id,
            symbol=position.symbol,
            side="SELL",
            quantity=position.quantity,
            price=None,
            order_type="MARKET",
        )
        return PositionExitDecision(
            order_intent_id=order_intent_id,
            request=request,
            reason=reason,
            trigger_price=trigger,
            current_price=current_price,
            pnl_pct=pnl_pct,
        )

    def _order_intent_id(self, symbol: str, reason: str, now: datetime) -> str:
        day = now.astimezone(timezone.utc).strftime("%Y%m%d")
        return f"OI-{day}-MON-{symbol}-{reason}"

    async def _fetch_open_positions(self) -> list[MonitoredPosition]:
        if not self.dsn:
            return []

        conn = await asyncpg.connect(self.dsn)
        try:
            rows = await conn.fetch(
                f"""
                SELECT symbol, quantity, average_price
                FROM (
                    SELECT DISTINCT ON (symbol)
                        symbol,
                        quantity,
                        average_price,
                        snapshot_time
                    FROM {self.schema}.position_snapshots
                    WHERE account_id = $1
                    ORDER BY symbol, snapshot_time DESC
                ) latest
                WHERE quantity > 0
                """,
                self.account_id,
            )
        finally:
            await conn.close()

        return [
            MonitoredPosition(
                account_id=self.account_id,
                symbol=str(row["symbol"]),
                quantity=_to_decimal(row["quantity"]),
                average_price=_to_decimal(row["average_price"]),
            )
            for row in rows
        ]

    async def _fetch_latest_market_price(self, symbol: str) -> Decimal:
        if self.broker is not None and hasattr(self.broker, "get_market_tick"):
            try:
                payload = await self.broker.get_market_tick(symbol)  # type: ignore[misc]
                if isinstance(payload, dict):
                    return _to_decimal(payload.get("price"))
                return _to_decimal(payload)
            except Exception:
                pass

        if not self.dsn:
            return Decimal("0")

        conn = await asyncpg.connect(self.dsn)
        try:
            row = await conn.fetchrow(
                f"""
                SELECT close_price
                FROM {self.schema}.ohlcv
                WHERE symbol = $1
                ORDER BY bucket_start DESC
                LIMIT 1
                """,
                symbol,
            )
        finally:
            await conn.close()

        if row is None:
            return Decimal("0")
        return _to_decimal(row["close_price"])

    async def _exit_intent_exists(self, order_intent_id: str) -> bool:
        if order_intent_id in self._emitted:
            return True
        if not self.dsn:
            return False

        conn = await asyncpg.connect(self.dsn)
        try:
            value = await conn.fetchval(
                f"""
                SELECT 1
                FROM {self.schema}.order_intents
                WHERE order_intent_id = $1
                LIMIT 1
                """,
                order_intent_id,
            )
        finally:
            await conn.close()
        return value is not None

    async def _publish_exit(self, decision: PositionExitDecision) -> None:
        self._emitted.add(decision.order_intent_id)
        await self.bus.publish(
            OrderIntentEvent(
                event_type=EventType.ORDER_INTENT,
                request=decision.request,
                payload={
                    "source": "position_monitor",
                    "reason": decision.reason,
                    "trigger_price": str(decision.trigger_price),
                    "current_price": str(decision.current_price),
                    "pnl_pct": str(decision.pnl_pct),
                },
            )
        )
        await self.bus.publish(
            RiskEvent(
                event_type=EventType.RISK,
                order_intent_id=decision.order_intent_id,
                stage="position_monitor",
                passed=False,
                reason=decision.reason,
                payload={
                    "symbol": decision.request.symbol,
                    "side": decision.request.side,
                    "quantity": str(decision.request.quantity),
                    "current_price": str(decision.current_price),
                },
            )
        )

    async def run_once(self) -> list[PositionExitDecision]:
        decisions: list[PositionExitDecision] = []
        now = datetime.now(timezone.utc)
        for position in await self._fetch_open_positions():
            current_price = await self._fetch_latest_market_price(position.symbol)
            decision = self.evaluate(position, current_price, now=now)
            if decision is None:
                continue
            if await self._exit_intent_exists(decision.order_intent_id):
                continue
            await self._publish_exit(decision)
            decisions.append(decision)
        return decisions
