"""Detect strategy drift between live and backtest performance."""

from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import asyncpg
import httpx

from core.system_state import SystemStateManager


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _sqrt(value: Decimal) -> Decimal:
    if value <= Decimal("0"):
        return Decimal("0")
    try:
        return value.sqrt()
    except Exception:
        return Decimal("0")


def _format_percent(value: Decimal) -> str:
    return f"{value * Decimal('100'):.4f}%"


@dataclass(frozen=True)
class _StrategyStat:
    sharpe: Decimal
    win_rate: Decimal
    trade_count: int


class StrategyDriftDetector:
    def __init__(
        self,
        *,
        state_mgr: SystemStateManager | None = None,
        dsn: str | None = None,
        environment: str | None = None,
    ) -> None:
        self.state_mgr = state_mgr
        self.dsn = dsn or os.getenv("DATABASE_URL")
        self.environment = (environment or os.getenv("ENVIRONMENT", "paper")).lower()

    @property
    def schema(self) -> str:
        return f"trading_{self.environment}"

    def _risk_limit_exceeded(self, live_sharpe: Decimal, backtest_sharpe: Decimal, live_wr: Decimal, backtest_wr: Decimal) -> bool:
        return (live_sharpe - backtest_sharpe).copy_abs() > Decimal("0.5") or (live_wr - backtest_wr).copy_abs() > Decimal("0.15")

    async def _fetch_live_stats(self, conn: asyncpg.Connection) -> dict[str, _StrategyStat]:
        candidates = (
            f"""
            SELECT
                COALESCE(strategy_id, 'UNKNOWN') AS strategy_id,
                COALESCE(AVG(-slippage_pct), 0) AS mean_ret,
                COALESCE(STDDEV_POP(-slippage_pct), 0) AS std_ret,
                COUNT(*) AS trade_count,
                SUM(CASE WHEN -slippage_pct > 0 THEN 1 ELSE 0 END) AS win_count
            FROM {self.schema}.slippage_records
            WHERE filled_at >= NOW() - INTERVAL '7 days'
              AND strategy_id IS NOT NULL
            GROUP BY COALESCE(strategy_id, 'UNKNOWN')
            """,
            f"""
            SELECT
                COALESCE(strategy_id, 'UNKNOWN') AS strategy_id,
                COALESCE(AVG(realized_pnl), 0) AS mean_ret,
                COALESCE(STDDEV_POP(realized_pnl), 0) AS std_ret,
                COUNT(*) AS trade_count,
                SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS win_count
            FROM {self.schema}.closed_trades
            WHERE exited_at >= NOW() - INTERVAL '7 days'
              AND strategy_id IS NOT NULL
            GROUP BY COALESCE(strategy_id, 'UNKNOWN')
            """,
        )

        for query in candidates:
            try:
                rows = await conn.fetch(query)
                break
            except Exception:
                rows = []
                continue

        out: dict[str, _StrategyStat] = {}
        for row in rows:
            strategy_id = str(row["strategy_id"])
            trade_count = int(row["trade_count"] or 0)
            if trade_count <= 0:
                continue
            mean_ret = _to_decimal(row["mean_ret"])
            std_ret = _to_decimal(row["std_ret"])
            sharpe = Decimal("0")
            if std_ret > Decimal("0"):
                sharpe = (mean_ret / std_ret) * Decimal("252").sqrt()
            win_rate = _to_decimal(row["win_count"]) / _to_decimal(trade_count)
            out[strategy_id] = _StrategyStat(sharpe=sharpe, win_rate=win_rate, trade_count=trade_count)
        return out

    async def _fetch_backtest_stats(self, conn: asyncpg.Connection) -> dict[str, _StrategyStat]:
        query_variants = (
            f"""
            SELECT strategy_id, sharpe_ratio, win_rate
            FROM {self.schema}.strategy_backtest_metrics
            ORDER BY strategy_id, report_date DESC
            """,
            f"""
            SELECT strategy_id, sharpe_ratio, win_rate
            FROM {self.schema}.strategy_metrics
            ORDER BY strategy_id, created_at DESC
            """,
            f"""
            SELECT strategy_id, sharpe_ratio, win_rate
            FROM {self.schema}.backtest_metrics
            ORDER BY strategy_id, evaluated_at DESC
            """,
        )

        rows = []
        for query in query_variants:
            try:
                rows = await conn.fetch(query)
                if rows:
                    break
            except Exception:
                rows = []
                continue

        latest: dict[str, _StrategyStat] = {}
        seen: set[str] = set()
        for row in rows:
            strategy_id = str(row["strategy_id"])
            if strategy_id in seen:
                continue
            seen.add(strategy_id)
            sharpe = _to_decimal(row.get("sharpe_ratio"))
            win_rate = _to_decimal(row.get("win_rate"))
            latest[strategy_id] = _StrategyStat(sharpe=sharpe, win_rate=win_rate, trade_count=0)
        return latest

    async def _ensure_mode_schema(self, conn: asyncpg.Connection) -> None:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.strategy_modes (
                strategy_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                weight NUMERIC(10,6) DEFAULT 1,
                updated_by TEXT NOT NULL DEFAULT 'system',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

    async def _update_strategy_action(self, conn: asyncpg.Connection, strategy_id: str, action: str, reason: str) -> None:
        await self._ensure_mode_schema(conn)
        if action == "reduce_weight_50pct":
            current = await conn.fetchval(
                f"SELECT COALESCE(weight, 1) FROM {self.schema}.strategy_modes WHERE strategy_id = $1",
                strategy_id,
            )
            current_weight = _to_decimal(current)
            if current_weight <= Decimal("0"):
                current_weight = Decimal("1")
            next_weight = (current_weight * Decimal("0.5")).quantize(Decimal("0.000001"))
            await conn.execute(
                f"""
                INSERT INTO {self.schema}.strategy_modes (strategy_id, mode, is_active, weight, updated_by)
                VALUES ($1, 'LIVE_APPROVAL', TRUE, $2, 'strategy_drift')
                ON CONFLICT (strategy_id) DO UPDATE
                SET mode = 'LIVE_APPROVAL', weight = EXCLUDED.weight, is_active = TRUE, updated_by = 'strategy_drift', updated_at = NOW()
                """,
                strategy_id,
                str(next_weight),
            )
            if next_weight < Decimal("0.05"):
                await conn.execute(
                    f"""
                    UPDATE {self.schema}.strategy_modes
                    SET mode = 'LIVE_APPROVAL', is_active = TRUE, updated_at = NOW(), updated_by = 'strategy_drift'
                    WHERE strategy_id = $1
                    """,
                    strategy_id,
                )
        else:
            await conn.execute(
                f"""
                INSERT INTO {self.schema}.strategy_modes (strategy_id, mode, is_active, updated_by)
                VALUES ($1, 'LIVE_APPROVAL', TRUE, 'strategy_drift')
                ON CONFLICT (strategy_id) DO UPDATE
                SET mode = 'LIVE_APPROVAL', is_active = TRUE, updated_by = 'strategy_drift', updated_at = NOW()
                """,
                strategy_id,
            )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.strategy_drift_log (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                strategy_id TEXT NOT NULL,
                backtest_sharpe NUMERIC(10,6),
                live_sharpe NUMERIC(10,6),
                sharpe_diff NUMERIC(10,6),
                backtest_win_rate NUMERIC(10,6),
                live_win_rate NUMERIC(10,6),
                action_taken TEXT,
                detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                reason TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            f"""
            INSERT INTO {self.schema}.strategy_drift_log (
                strategy_id,
                backtest_sharpe,
                live_sharpe,
                sharpe_diff,
                backtest_win_rate,
                live_win_rate,
                action_taken,
                reason
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            strategy_id,
            str(reason.get("backtest_sharpe", Decimal("0"))),
            str(reason.get("live_sharpe", Decimal("0"))),
            str(reason.get("sharpe_diff", Decimal("0"))),
            str(reason.get("backtest_wr", Decimal("0"))),
            str(reason.get("live_wr", Decimal("0"))),
            action,
            reason.get("message", ""),
        )

    def _send_slack(self, message: str) -> None:
        webhook = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook:
            return

        payload = {"text": message}
        try:
            httpx.post(webhook, json=payload, timeout=8)
        except Exception:
            return

    async def check_all_strategies(self) -> list[dict[str, Any]]:
        if not self.dsn:
            return []

        conn = await asyncpg.connect(self.dsn)
        try:
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.strategy_drift_log (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    strategy_id TEXT,
                    backtest_sharpe NUMERIC(10,6),
                    live_sharpe NUMERIC(10,6),
                    sharpe_diff NUMERIC(10,6),
                    backtest_win_rate NUMERIC(10,6),
                    live_win_rate NUMERIC(10,6),
                    action_taken TEXT,
                    detected_at TIMESTAMPTZ DEFAULT NOW(),
                    reason TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )

            live = await self._fetch_live_stats(conn)
            backtest = await self._fetch_backtest_stats(conn)
            events: list[dict[str, Any]] = []

            for strategy_id in sorted(set(live) & set(backtest)):
                live_stats = live[strategy_id]
                backtest_stats = backtest[strategy_id]

                if not self._risk_limit_exceeded(
                    live_stats.sharpe,
                    backtest_stats.sharpe,
                    live_stats.win_rate,
                    backtest_stats.win_rate,
                ):
                    continue

                action = "reduce_weight_50pct"
                sharpe_diff = live_stats.sharpe - backtest_stats.sharpe
                wr_diff = live_stats.win_rate - backtest_stats.win_rate

                reason = {
                    "strategy_id": strategy_id,
                    "backtest_sharpe": backtest_stats.sharpe,
                    "live_sharpe": live_stats.sharpe,
                    "sharpe_diff": sharpe_diff,
                    "backtest_wr": backtest_stats.win_rate,
                    "live_wr": live_stats.win_rate,
                    "message": "live performance drift from backtest",
                }

                await self._update_strategy_action(conn, strategy_id, action, reason)

                text = (
                    f"[strategy_drift] {strategy_id}: "
                    f"backtest_sharpe={backtest_stats.sharpe} live_sharpe={live_stats.sharpe} "
                    f"diff={sharpe_diff}; backtest_wr={_format_percent(backtest_stats.win_rate)} "
                    f"live_wr={_format_percent(live_stats.win_rate)}"
                )
                self._send_slack(text)
                events.append(reason)

            return events
        finally:
            await conn.close()
