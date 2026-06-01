"""Monitors system signals and applies state transitions for anomalies."""

from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from collections.abc import Callable, Awaitable
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from core.system_state import SystemState, SystemStateManager


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


class AnomalyDetector:
    """Collect anomaly signals and push transitions through state manager."""

    def __init__(
        self,
        state_mgr: SystemStateManager,
        *,
        schema: str | None = None,
        market_data_delay_seconds: Decimal = Decimal("10"),
        websocket_gap_seconds: Decimal = Decimal("15"),
        broker_delay_limit: int = 3,
        balance_failure_limit: int = 3,
        llm_parse_failure_rate_threshold: Decimal = Decimal("0.30"),
        llm_parse_window_seconds: int = 60,
        llm_parse_failures_for_window: int = 3,
        rapid_order_window_seconds: int = 30,
        rapid_order_threshold: int = 5,
        daily_loss_threshold: Decimal = Decimal("0.10"),
        run_interval_seconds: int = 10,
        loss_sampler: Callable[[], Awaitable[Decimal]] | None = None,
    ) -> None:
        self._state_mgr = state_mgr
        self.market_data_delay_seconds = _to_decimal(market_data_delay_seconds)
        self.websocket_gap_seconds = _to_decimal(websocket_gap_seconds)
        self.broker_delay_limit = int(broker_delay_limit)
        self.balance_failure_limit = int(balance_failure_limit)
        self.llm_parse_failure_rate_threshold = _to_decimal(llm_parse_failure_rate_threshold)
        self.llm_parse_window_seconds = int(llm_parse_window_seconds)
        self.llm_parse_failures_for_window = int(llm_parse_failures_for_window)
        self.rapid_order_window_seconds = int(rapid_order_window_seconds)
        self.rapid_order_threshold = int(rapid_order_threshold)
        self.daily_loss_threshold = _to_decimal(daily_loss_threshold)
        self.run_interval_seconds = int(run_interval_seconds)
        self._loss_sampler = loss_sampler
        self.schema = schema

        self.metrics: dict[str, Any] = {
            "last_market_data_ts": None,
            "last_heartbeat_ts": None,
            "consecutive_broker_delays": 0,
            "consecutive_balance_failures": 0,
            "llm_parse_failures_window": [],
            "llm_parse_attempts_window": [],
            "daily_loss_pct": Decimal("0"),
            "recent_orders": [],
        }

    def update_market_data(self, *, occurred_at: datetime | None = None) -> None:
        self.metrics["last_market_data_ts"] = occurred_at or _utcnow()

    def update_heartbeat(self, *, occurred_at: datetime | None = None) -> None:
        self.metrics["last_heartbeat_ts"] = occurred_at or _utcnow()

    def record_broker_delay(self, delayed: bool) -> None:
        if delayed:
            self.metrics["consecutive_broker_delays"] = int(self.metrics["consecutive_broker_delays"]) + 1
        else:
            self.metrics["consecutive_broker_delays"] = 0

    def record_balance_check(self, success: bool) -> None:
        if success:
            self.metrics["consecutive_balance_failures"] = 0
        else:
            self.metrics["consecutive_balance_failures"] = int(self.metrics["consecutive_balance_failures"]) + 1

    def record_llm_parse(self, *, success: bool, ts: datetime | None = None) -> None:
        now = ts or _utcnow()
        attempts = list(self.metrics["llm_parse_attempts_window"])
        failures = list(self.metrics["llm_parse_failures_window"])
        attempts.append(now)
        if not success:
            failures.append(now)
        self.metrics["llm_parse_attempts_window"] = attempts
        self.metrics["llm_parse_failures_window"] = failures

    def record_order(self, symbol: str, direction: str, *, ts: datetime | None = None) -> None:
        self.metrics["recent_orders"].append((str(symbol), str(direction).upper(), ts or _utcnow()))

    def update_daily_loss_pct(self, value: Decimal) -> None:
        self.metrics["daily_loss_pct"] = _to_decimal(value)

    async def _refresh_daily_loss(self) -> None:
        if self._loss_sampler is None:
            return
        try:
            value = await self._loss_sampler()
            self.update_daily_loss_pct(_to_decimal(value))
        except Exception:
            return

    def _trim(self, now: datetime) -> None:
        failures = list(self.metrics["llm_parse_failures_window"])
        attempts = list(self.metrics["llm_parse_attempts_window"])
        cutoff = now - timedelta(seconds=self.llm_parse_window_seconds)
        while failures and failures[0] < cutoff:
            failures.pop(0)
        while attempts and attempts[0] < cutoff:
            attempts.pop(0)

        cutoff_order = now - timedelta(seconds=self.rapid_order_window_seconds)
        orders = list(self.metrics["recent_orders"])
        while orders and orders[0][2] < cutoff_order:
            orders.pop(0)

        self.metrics["llm_parse_failures_window"] = failures
        self.metrics["llm_parse_attempts_window"] = attempts
        self.metrics["recent_orders"] = orders

    def _components(self, now: datetime) -> dict[str, bool]:
        components = {
            "market_data": False,
            "ws_heartbeat": False,
            "broker": False,
            "balance": False,
            "llm": False,
        }

        last_market = self.metrics.get("last_market_data_ts")
        if isinstance(last_market, datetime):
            market_gap = _to_decimal((now - last_market).total_seconds())
            components["market_data"] = market_gap >= self.market_data_delay_seconds

        last_heartbeat = self.metrics.get("last_heartbeat_ts")
        if isinstance(last_heartbeat, datetime):
            heartbeat_gap = _to_decimal((now - last_heartbeat).total_seconds())
            components["ws_heartbeat"] = heartbeat_gap >= self.websocket_gap_seconds

        components["broker"] = int(self.metrics.get("consecutive_broker_delays", 0)) >= self.broker_delay_limit
        components["balance"] = int(self.metrics.get("consecutive_balance_failures", 0)) >= self.balance_failure_limit

        attempts = list(self.metrics.get("llm_parse_attempts_window", []))
        failures = list(self.metrics.get("llm_parse_failures_window", []))
        if attempts:
            rate = _to_decimal(len(failures)) / _to_decimal(len(attempts))
            components["llm"] = rate >= self.llm_parse_failure_rate_threshold
        else:
            components["llm"] = int(len(failures)) >= self.llm_parse_failures_for_window

        return components

    def _rapid_orders(self) -> tuple[bool, str]:
        orders = list(self.metrics.get("recent_orders", []))
        counts = Counter((symbol, direction) for symbol, direction, _ in orders)
        for (symbol, direction), count in counts.items():
            if count >= self.rapid_order_threshold:
                return True, f"rapid_orders_{symbol}_{direction}_{count}"

        if len(orders) >= self.rapid_order_threshold:
            return True, f"rapid_orders_burst_{len(orders)}"

        return False, ""

    async def tick(self) -> None:
        now = _utcnow()
        await self._refresh_daily_loss()
        self._trim(now)
        components = self._components(now)
        is_brownout = sum(1 for value in components.values() if value) >= 2

        daily_loss = _to_decimal(self.metrics.get("daily_loss_pct", Decimal("0")))
        if daily_loss >= self.daily_loss_threshold:
            self._state_mgr.transition_to(SystemState.EMERGENCY_STOP, reason=f"daily_loss_pct={daily_loss}")
            return

        rapid, rapid_reason = self._rapid_orders()
        if rapid:
            self._state_mgr.transition_to(SystemState.EMERGENCY_STOP, reason=f"{rapid_reason}")
            return

        if is_brownout:
            self._state_mgr.transition_to(SystemState.BROWNOUT, reason="multiple_components_degraded")
            return

        if components["llm"]:
            self._state_mgr.transition_to(SystemState.DEGRADED_LLM, reason="llm_parse_rate_high")
            return

        if components["market_data"] or components["ws_heartbeat"] or components["broker"] or components["balance"]:
            self._state_mgr.transition_to(SystemState.DEGRADED_MARKET, reason="market_or_broker_or_balance_degraded")
            return

        self._state_mgr.transition_to(SystemState.NORMAL, reason="anomaly_signals_cleared")

    async def run(self) -> None:
        while True:
            await self.tick()
            await asyncio.sleep(self.run_interval_seconds)
