"""Reconcile broker positions against internal position snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import json

import asyncpg

from brokers.kiwoom_rest_kr_live import KiwoomRestKrLiveAdapter
from brokers.kiwoom_rest_kr_mock import KiwoomRestKrMockAdapter
from brokers.simulated import SimulatedBrokerAdapter
from core.events.bus import RedisStreamBus
from core.events.schemas import EventType, RiskEvent


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _select_broker() -> object:
    adapter = __import__("os").getenv("BROKER_ADAPTER", "simulated").lower()
    if adapter == "kiwoom_mock":
        return KiwoomRestKrMockAdapter()
    if adapter == "kiwoom_live":
        return KiwoomRestKrLiveAdapter()
    return SimulatedBrokerAdapter()


class ReconciliationMonitor:
    def __init__(
        self,
        schema: str,
        broker: object | None = None,
        redis_url: str = "redis://localhost:6379/0",
        dsn: str | None = None,
        account_id: str = "default",
        environment: str = "paper",
    ) -> None:
        self.schema = schema
        self.broker = broker or _select_broker()
        self.dsn = dsn
        self.account_id = account_id
        self.environment = environment
        self.bus = RedisStreamBus(redis_url=redis_url, stream_prefix=f"{environment}.events")

    async def _fetch_internal_positions(self) -> dict[str, Decimal]:
        if not self.dsn:
            return {}

        conn = await asyncpg.connect(self.dsn)
        try:
            rows = await conn.fetch(
                f"""
                SELECT symbol, quantity
                FROM (
                    SELECT DISTINCT ON (symbol)
                        symbol,
                        quantity,
                        snapshot_time
                    FROM {self.schema}.position_snapshots
                    WHERE account_id = $1
                    ORDER BY symbol, snapshot_time DESC
                ) AS latest
                """,
                self.account_id,
            )
            return {str(row["symbol"]): _to_decimal(row["quantity"]) for row in rows}
        finally:
            await conn.close()

    async def _fetch_broker_positions(self) -> dict[str, Decimal]:
        if hasattr(self.broker, "get_positions") and callable(self.broker.get_positions):
            positions = await self.broker.get_positions(self.account_id)  # type: ignore[misc]
            if isinstance(positions, dict):
                return {str(symbol): _to_decimal(qty) for symbol, qty in positions.items()}

        mapping = getattr(self.broker, "_positions", {})
        if isinstance(mapping, dict):
            values: dict[str, Decimal] = {}
            for key, value in mapping.items():
                if isinstance(key, tuple) and len(key) >= 2:
                    account_id, symbol = key[0], key[1]
                    if str(account_id) != self.account_id:
                        continue
                    values[str(symbol)] = _to_decimal(getattr(value, "quantity", value))
                else:
                    values[str(key)] = _to_decimal(value)
            return values

        return {}

    async def _latest_price(self, symbol: str) -> Decimal:
        if not self.dsn:
            return Decimal("0")

        conn = await asyncpg.connect(self.dsn)
        try:
            row = await conn.fetchrow(
                f"""
                SELECT average_price
                FROM {self.schema}.position_snapshots
                WHERE account_id = $1 AND symbol = $2
                ORDER BY snapshot_time DESC
                LIMIT 1
                """,
                self.account_id,
                symbol,
            )
            if row is None:
                return Decimal("0")
            return _to_decimal(row["average_price"])
        finally:
            await conn.close()

    async def _persist_position_snapshot(self, symbol: str, quantity: Decimal) -> None:
        if not self.dsn:
            return

        avg_price = await self._latest_price(symbol)
        conn = await asyncpg.connect(self.dsn)
        try:
            await conn.execute(
                f"""
                INSERT INTO {self.schema}.position_snapshots (
                    account_id,
                    symbol,
                    quantity,
                    average_price,
                    realized_pnl,
                    snapshot_time
                ) VALUES ($1, $2, $3, $4, $5, $6)
                """,
                self.account_id,
                symbol,
                str(quantity),
                str(avg_price),
                str(Decimal("0")),
                datetime.now(timezone.utc),
            )
        finally:
            await conn.close()

    async def _write_reconciliation_log(
        self,
        symbol_code: str,
        internal_qty: Decimal,
        broker_qty: Decimal,
        diff_qty: Decimal,
        severity: str,
        action_taken: str,
    ) -> None:
        if not self.dsn:
            return

        conn = await asyncpg.connect(self.dsn)
        try:
            await conn.execute(
                f"""
                INSERT INTO {self.schema}.reconciliation_log (
                    symbol_market,
                    symbol_code,
                    internal_qty,
                    broker_qty,
                    diff_qty,
                    severity,
                    action_taken
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                "KR",
                symbol_code,
                str(internal_qty),
                str(broker_qty),
                str(diff_qty),
                severity,
                action_taken,
            )
        finally:
            await conn.close()

    async def _publish_reconciliation_log(self, payload: dict[str, Any]) -> None:
        if self.bus._client is not None:
            await self.bus._client.xadd(
                f"{self.environment}.events.reconciliation",
                {"payload": json.dumps(payload, ensure_ascii=False)},
            )

    async def _publish_emergency_stop(self, symbol: str, diff: Decimal) -> None:
        await self.bus.publish(
            RiskEvent(
                event_type=EventType.RISK,
                payload={
                    "event_type": "reconciliation",
                    "symbol": symbol,
                    "diff": str(diff),
                },
                order_intent_id="",
                stage="reconciliation",
                passed=False,
                reason=f"position mismatch {symbol}: {diff}",
            )
        )

        if self.bus._client is not None:
            await self.bus._client.xadd(
                f"{self.environment}.events.system_state",
                {
                    "payload": json.dumps(
                        {
                            "event_type": "system_state",
                            "state": "EMERGENCY_STOP",
                            "reason": f"position mismatch {symbol}: {diff}",
                        },
                        ensure_ascii=False,
                    )
                },
            )

    async def check_once(self) -> list[dict[str, Any]]:
        internal = await self._fetch_internal_positions()
        broker = await self._fetch_broker_positions()

        symbols = set(internal.keys()) | set(broker.keys())
        results: list[dict[str, Any]] = []

        for symbol in sorted(symbols):
            internal_qty = _to_decimal(internal.get(symbol, Decimal("0")))
            broker_qty = _to_decimal(broker.get(symbol, Decimal("0")))
            diff = broker_qty - internal_qty
            abs_diff = diff.copy_abs()

            if abs_diff >= Decimal("1"):
                severity = "critical"
                action = "emergency_stop"
                await self._persist_position_snapshot(symbol, broker_qty)
                await self._publish_emergency_stop(symbol, diff)
            elif abs_diff > Decimal("0"):
                severity = "warning"
                action = "positions_updated_from_broker"
                await self._persist_position_snapshot(symbol, broker_qty)
            else:
                severity = "ok"
                action = "no_action"

            await self._write_reconciliation_log(
                symbol_code=symbol,
                internal_qty=internal_qty,
                broker_qty=broker_qty,
                diff_qty=diff,
                severity=severity,
                action_taken=action,
            )
            await self._publish_reconciliation_log(
                {
                    "symbol": symbol,
                    "internal_qty": str(internal_qty),
                    "broker_qty": str(broker_qty),
                    "diff_qty": str(diff),
                    "severity": severity,
                    "action": action,
                    "reconciled_at": datetime.now(timezone.utc).isoformat(),
                }
            )

            results.append(
                {
                    "symbol": symbol,
                    "internal_qty": internal_qty,
                    "broker_qty": broker_qty,
                    "diff_qty": diff,
                    "severity": severity,
                }
            )

        return results

    async def run(self) -> None:
        while True:
            await self.check_once()
            await asyncio_sleep_seconds(900)


def json_dumps(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False)


async def asyncio_sleep_seconds(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)
