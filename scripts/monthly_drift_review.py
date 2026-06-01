"""Monthly strategy drift review script.

Compares backtest vs live performance for each strategy over the past
30 days. Prints PR-style weight-reduction proposals for high-drift
strategies and records findings to strategy_drift_log.

Usage::

    python scripts/monthly_drift_review.py

Environment variables:
    DATABASE_URL, ENVIRONMENT (default: paper)
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg


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


def _environment() -> str:
    return os.getenv("ENVIRONMENT", "paper").lower()


def _schema() -> str:
    return f"trading_{_environment()}"


@dataclass
class _StratStats:
    strategy_id: str
    sharpe: Decimal
    win_rate: Decimal
    trade_count: int


def _compute_stats(rows: list[dict[str, Any]]) -> dict[str, _StratStats]:
    """Group rows by strategy_id and compute sharpe + win_rate."""
    from collections import defaultdict

    groups: dict[str, list[Decimal]] = defaultdict(list)
    for row in rows:
        sid = str(row.get("strategy_id") or "UNKNOWN")
        groups[sid].append(_to_decimal(row.get("pnl") or row.get("realized_pnl") or "0"))

    result: dict[str, _StratStats] = {}
    for sid, pnls in groups.items():
        n = len(pnls)
        total = sum(pnls)
        mean = total / Decimal(n) if n > 0 else Decimal("0")
        wins = sum(1 for p in pnls if p > Decimal("0"))
        win_rate = Decimal(wins) / Decimal(n) if n > 0 else Decimal("0")
        variance = (
            sum((p - mean) ** 2 for p in pnls) / Decimal(n) if n > 0 else Decimal("0")
        )
        std = _sqrt(variance)
        sharpe = mean / std if std > Decimal("0") else Decimal("0")
        result[sid] = _StratStats(
            strategy_id=sid,
            sharpe=sharpe,
            win_rate=win_rate,
            trade_count=n,
        )
    return result


async def _fetch_live_stats(conn: asyncpg.Connection, schema: str) -> dict[str, _StratStats]:
    queries = [
        f"""
        SELECT
            COALESCE(strategy_id, 'UNKNOWN') AS strategy_id,
            COALESCE(pnl, 0) AS pnl
        FROM {schema}.journal_entries
        WHERE exit_at >= NOW() - INTERVAL '30 days'
          AND strategy_id IS NOT NULL
        """,
        f"""
        SELECT
            COALESCE(strategy_id, 'UNKNOWN') AS strategy_id,
            (-slippage_pct) AS pnl
        FROM {schema}.slippage_records
        WHERE filled_at >= NOW() - INTERVAL '30 days'
          AND strategy_id IS NOT NULL
        """,
    ]
    for query in queries:
        try:
            rows = await conn.fetch(query)
            if rows:
                return _compute_stats([dict(r) for r in rows])
        except Exception:
            continue
    return {}


async def _fetch_backtest_stats(conn: asyncpg.Connection, schema: str) -> dict[str, _StratStats]:
    """Load backtest stats from performance_attribution table (oldest available)."""
    try:
        rows = await conn.fetch(
            f"""
            SELECT
                strategy_id,
                sharpe,
                win_rate,
                trade_count
            FROM {schema}.performance_attribution
            WHERE period_start >= NOW() - INTERVAL '90 days'
              AND strategy_id IS NOT NULL
            ORDER BY period_start ASC
            """
        )
        result: dict[str, _StratStats] = {}
        for row in rows:
            sid = str(row["strategy_id"])
            if sid not in result:
                result[sid] = _StratStats(
                    strategy_id=sid,
                    sharpe=_to_decimal(row["sharpe"]),
                    win_rate=_to_decimal(row["win_rate"]),
                    trade_count=int(row["trade_count"] or 0),
                )
        return result
    except Exception:
        return {}


async def _insert_drift_log(
    conn: asyncpg.Connection,
    schema: str,
    strategy_id: str,
    backtest: _StratStats,
    live: _StratStats,
    action: str,
) -> None:
    sharpe_diff = (live.sharpe - backtest.sharpe).copy_abs()
    try:
        await conn.execute(
            f"""
            INSERT INTO {schema}.strategy_drift_log (
                strategy_id, backtest_sharpe, live_sharpe, sharpe_diff,
                backtest_win_rate, live_win_rate, action_taken
            ) VALUES ($1,$2,$3,$4,$5,$6,$7)
            """,
            strategy_id,
            float(backtest.sharpe),
            float(live.sharpe),
            float(sharpe_diff),
            float(backtest.win_rate),
            float(live.win_rate),
            action,
        )
    except Exception as exc:
        print(f"[monthly_drift_review] DB insert failed: {exc}")


def _print_pr_diff(
    strategy_id: str,
    backtest: _StratStats,
    live: _StratStats,
    proposed_weight_reduction: Decimal,
) -> None:
    """Print a PR-style diff proposal to stdout."""
    sharpe_diff = live.sharpe - backtest.sharpe
    wr_diff = (live.win_rate - backtest.win_rate) * Decimal("100")
    print(f"\n--- configs/strategies/{strategy_id}.yaml (current)")
    print(f"+++ configs/strategies/{strategy_id}.yaml (proposed)")
    print(f"@@ strategy drift detected @@")
    print(f" # backtest sharpe: {backtest.sharpe:.4f}, live sharpe: {live.sharpe:.4f} (diff: {sharpe_diff:+.4f})")
    print(f" # backtest win_rate: {backtest.win_rate*Decimal('100'):.2f}%, live win_rate: {live.win_rate*Decimal('100'):.2f}% (diff: {wr_diff:+.2f}pp)")
    print(f"-  weight: 1.0  # original")
    print(f"+  weight: {float(Decimal('1.0') - proposed_weight_reduction):.2f}  # auto-reduced by {float(proposed_weight_reduction)*100:.0f}% due to drift")
    print()


async def main() -> None:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("[monthly_drift_review] DATABASE_URL not set, exiting.")
        return

    schema = _schema()
    conn = await asyncpg.connect(dsn)
    try:
        live_stats = await _fetch_live_stats(conn, schema)
        backtest_stats = await _fetch_backtest_stats(conn, schema)
    finally:
        await conn.close()

    if not live_stats:
        print("[monthly_drift_review] No live trade data found for the past 30 days.")
        return

    sharpe_threshold = Decimal("0.5")
    win_rate_threshold = Decimal("0.15")
    drift_found = False

    conn2 = await asyncpg.connect(dsn)
    try:
        for strategy_id, live in sorted(live_stats.items()):
            backtest = backtest_stats.get(strategy_id)
            if backtest is None:
                print(f"[monthly_drift_review] No backtest baseline for {strategy_id}, skipping.")
                continue

            sharpe_diff = (live.sharpe - backtest.sharpe).copy_abs()
            wr_diff = (live.win_rate - backtest.win_rate).copy_abs()

            if sharpe_diff > sharpe_threshold or wr_diff > win_rate_threshold:
                drift_found = True
                weight_reduction = Decimal("0.5")
                action = f"propose_50pct_weight_reduction (sharpe_diff={sharpe_diff:.4f}, wr_diff={wr_diff:.4f})"
                _print_pr_diff(strategy_id, backtest, live, weight_reduction)
                await _insert_drift_log(conn2, schema, strategy_id, backtest, live, action)
            else:
                print(f"[monthly_drift_review] {strategy_id}: no significant drift detected (sharpe_diff={sharpe_diff:.4f}, wr_diff={wr_diff:.4f})")
    finally:
        await conn2.close()

    if not drift_found:
        print("[monthly_drift_review] All strategies within acceptable drift bounds.")

    print("\n[monthly_drift_review] Done.")


if __name__ == "__main__":
    asyncio.run(main())
