"""Background monitor worker: risk checks, slippage, and reconciliation."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import asyncpg

from agents.monitoring.anomaly_detector import AnomalyDetector
from agents.monitoring.position_monitor import PositionMonitor
from agents.monitoring.reconciliation import ReconciliationMonitor
from agents.monitoring.slippage_monitor import SlippageMonitor
from agents.monitoring.strategy_drift import StrategyDriftDetector
from core.events.bus import RedisStreamBus
from core.events.schemas import EventType, OrderIntentEvent, RiskEvent
from core.operating_mode import expire_approvals, handle_order_intent_published
from core.risk.limits import RiskManager
from core.system_state import SystemState, SystemStateMachine


class WorkerMonitor:
    def __init__(self) -> None:
        self.environment = os.getenv("ENVIRONMENT", "paper").lower()
        self.operating_mode = os.getenv("OPERATING_MODE", "READ_ONLY")
        self.redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        self.redis_prefix = os.getenv("REDIS_STREAM_PREFIX", f"{self.environment}.events")
        self.dsn = os.getenv("DATABASE_URL", "postgresql://stock:stock@localhost:5432/stock")
        self.account_id = os.getenv("ACCOUNT_ID", "default")
        self.schema = f"trading_{self.environment}"
        self.system_state = SystemStateMachine(
            environment=self.environment,
            redis_url=self.redis_url,
            stream_prefix=self.redis_prefix,
            dsn=self.dsn,
        )
        self._last_market_drift_day: date | None = None

        self.bus = RedisStreamBus(redis_url=self.redis_url, stream_prefix=self.redis_prefix)
        self.slippage = SlippageMonitor(
            stream_prefix=self.redis_prefix,
            schema=self.schema,
            redis_url=self.redis_url,
            dsn=self.dsn,
        )
        self.reconciliation = ReconciliationMonitor(
            schema=self.schema,
            redis_url=self.redis_url,
            dsn=self.dsn,
            account_id=self.account_id,
            environment=self.environment,
        )
        self.position_monitor = PositionMonitor(
            schema=self.schema,
            account_id=self.account_id,
            environment=self.environment,
            dsn=self.dsn,
            redis_url=self.redis_url,
            broker=self.reconciliation.broker,
        )
        self.risk = RiskManager()
        self.anomaly_detector = AnomalyDetector(self.system_state)
        self.strategy_drift = StrategyDriftDetector(
            state_mgr=self.system_state,
            dsn=self.dsn,
            environment=self.environment,
        )

    async def _risk_limits_loop(self) -> None:
        while True:
            await self._check_limits()
            await asyncio.sleep(60)

    async def _reconciliation_loop(self) -> None:
        while True:
            await self.reconciliation.check_once()
            await asyncio.sleep(15 * 60)

    async def _redis_heartbeat_loop(self) -> None:
        while True:
            await expire_approvals(environment=self.environment, redis_url=self.redis_url)
            await asyncio.sleep(5)

    async def _anomaly_loop(self) -> None:
        while True:
            await self.anomaly_detector.run()
            await asyncio.sleep(10)

    async def _strategy_drift_loop(self) -> None:
        while True:
            now = datetime.now(ZoneInfo("Asia/Seoul"))
            market_close = time(15, 20)
            if now.time() >= market_close:
                if self._last_market_drift_day != now.date():
                    await self.strategy_drift.check_all_strategies()
                    self._last_market_drift_day = now.date()
            await asyncio.sleep(60)

    async def _brownout_position_check(self) -> None:
        while True:
            if self.system_state.state == SystemState.BROWNOUT:
                await self.reconciliation.check_once()
            await asyncio.sleep(10)

    async def _position_monitor_loop(self) -> None:
        try:
            interval = int(os.getenv("POSITION_MONITOR_INTERVAL_SECONDS", "5"))
        except ValueError:
            interval = 5
        while True:
            await self.position_monitor.run_once()
            await asyncio.sleep(max(interval, 1))

    async def _order_intent_listener(self) -> None:
        async for event in self.bus.subscribe(EventType.ORDER_INTENT):
            if isinstance(event, OrderIntentEvent):
                await handle_order_intent_published(
                    event,
                    self.operating_mode,
                    environment=self.environment,
                    redis_url=self.redis_url,
                )
            else:
                payload = getattr(event, "payload", {}) or {}
                request = payload.get("request", {}) if isinstance(payload, dict) else {}
                order_intent_id = request.get("order_intent_id") if isinstance(request, dict) else None
                if order_intent_id:
                    await handle_order_intent_published(
                        str(order_intent_id),
                        self.operating_mode,
                        environment=self.environment,
                        redis_url=self.redis_url,
                    )

    async def _current_cash_balance(self) -> str:
        if not self.dsn:
            return "0"

        conn = await asyncpg.connect(self.dsn)
        try:
            row = await conn.fetchrow(
                f"""
                SELECT cash_balance
                FROM {self.schema}.cash_snapshots
                ORDER BY snapshot_time DESC
                LIMIT 1
                """,
            )
            if row is None:
                return "1000000"
            return str(row["cash_balance"])
        finally:
            await conn.close()

    async def _check_limits(self) -> None:
        if not self.dsn:
            return

        daily, weekly, _ = await self.risk.evaluate_loss_limits(
            account_id=self.account_id,
            schema=self.schema,
        )
        if daily.limit > 0 and daily.loss >= daily.limit:
            await self._publish_limit_stop("AUTO_EMERGENCY_STOP", "daily loss limit reached")
        elif weekly.limit > 0 and weekly.loss >= weekly.limit:
            await self._publish_limit_stop("ALERT_FORCE_LIVE_APPROVAL", "weekly loss limit reached")

    async def _publish_limit_stop(self, action: str, reason: str) -> None:
        payload: dict[str, str] = {
            "action": action,
            "reason": reason,
        }

        if action == "AUTO_EMERGENCY_STOP":
            self.system_state.emergency_stop("risk_limits")

        await self.bus.publish(
            RiskEvent(
                event_type=EventType.RISK,
                order_intent_id="",
                stage="limits",
                passed=False,
                reason=reason,
                payload=payload,
            )
        )

        if self.bus._client is not None and action == "AUTO_EMERGENCY_STOP":
            await self.bus._client.xadd(
                f"{self.environment}.events.system_state",
                {
                    "payload": json.dumps(
                        {
                            "event_type": "system_state",
                            "state": "EMERGENCY_STOP",
                            "reason": reason,
                            "action": action,
                        },
                        ensure_ascii=False,
                    )
                },
            )

    async def run(self) -> None:
        await asyncio.gather(
            self.slippage.run(),
            self._risk_limits_loop(),
            self._reconciliation_loop(),
            self._redis_heartbeat_loop(),
            self._brownout_position_check(),
            self._position_monitor_loop(),
            self._anomaly_loop(),
            self._strategy_drift_loop(),
            self._order_intent_listener(),
        )


async def main() -> None:
    monitor = WorkerMonitor()
    await monitor.run()


if __name__ == "__main__":
    asyncio.run(main())
