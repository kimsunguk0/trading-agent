"""Weekly performance attribution script.

Aggregates P&L by strategy and market regime for the past 7 days,
updates the performance_attribution table, generates a Claude Sonnet
summary, and sends it via Telegram.

Usage::

    python scripts/weekly_attribution.py

Environment variables required:
    DATABASE_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_ALLOWED_USER_IDS
    ENVIRONMENT (default: paper)
    ANTHROPIC_API_KEY
"""

from __future__ import annotations

import asyncio
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import asyncpg

try:
    from telegram import Bot
except ImportError:
    Bot = None  # type: ignore[assignment,misc]

try:
    import anthropic
except ImportError:
    anthropic = None  # type: ignore[assignment]


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


def _telegram_users() -> list[int]:
    raw = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "")
    return [int(item.strip()) for item in raw.split(",") if item.strip().isdigit()]


async def _fetch_weekly_trades(
    conn: asyncpg.Connection,
    schema: str,
) -> list[dict[str, Any]]:
    """Fetch closed trades from the past 7 days."""
    queries = [
        f"""
        SELECT
            COALESCE(strategy_id, 'UNKNOWN') AS strategy_id,
            COALESCE(regime_at_entry, 'UNKNOWN') AS regime,
            COALESCE(realized_pnl, 0) AS realized_pnl,
            COALESCE(pnl_pct, 0) AS pnl_pct
        FROM {schema}.journal_entries
        WHERE exit_at >= NOW() - INTERVAL '7 days'
          AND pnl IS NOT NULL
        """,
        f"""
        SELECT
            COALESCE(strategy_id, 'UNKNOWN') AS strategy_id,
            'UNKNOWN' AS regime,
            (-slippage_pct) AS realized_pnl,
            (-slippage_pct) AS pnl_pct
        FROM {schema}.slippage_records
        WHERE filled_at >= NOW() - INTERVAL '7 days'
          AND strategy_id IS NOT NULL
        """,
    ]
    for query in queries:
        try:
            rows = await conn.fetch(query)
            if rows:
                return [dict(r) for r in rows]
        except Exception:
            continue
    return []


def _compute_stats(
    trades: list[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Group trades by (strategy_id, regime) and compute stats."""
    from collections import defaultdict

    groups: dict[tuple[str, str], list[Decimal]] = defaultdict(list)
    for t in trades:
        key = (str(t.get("strategy_id", "UNKNOWN")), str(t.get("regime", "UNKNOWN")))
        groups[key].append(_to_decimal(t.get("realized_pnl", "0")))

    result: dict[tuple[str, str], dict[str, Any]] = {}
    for (strategy_id, regime), pnls in groups.items():
        n = len(pnls)
        total = sum(pnls)
        wins = sum(1 for p in pnls if p > Decimal("0"))
        win_rate = Decimal(wins) / Decimal(n) if n > 0 else Decimal("0")
        mean = total / Decimal(n) if n > 0 else Decimal("0")
        variance = (
            sum((p - mean) ** 2 for p in pnls) / Decimal(n)
            if n > 0
            else Decimal("0")
        )
        std = _sqrt(variance)
        sharpe = mean / std if std > Decimal("0") else Decimal("0")
        result[(strategy_id, regime)] = {
            "strategy_id": strategy_id,
            "regime": regime,
            "realized_pnl": total,
            "trade_count": n,
            "win_rate": win_rate,
            "sharpe": sharpe,
        }
    return result


async def _upsert_attribution(
    conn: asyncpg.Connection,
    schema: str,
    stats: dict[tuple[str, str], dict[str, Any]],
    period_start: date,
    period_end: date,
) -> None:
    """Insert new rows into performance_attribution."""
    for (strategy_id, regime), s in stats.items():
        try:
            await conn.execute(
                f"""
                INSERT INTO {schema}.performance_attribution (
                    period_start, period_end, strategy_id, regime,
                    realized_pnl, trade_count, win_rate, sharpe,
                    attribution_json
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                period_start,
                period_end,
                strategy_id,
                regime,
                float(s["realized_pnl"]),
                int(s["trade_count"]),
                float(s["win_rate"]),
                float(s["sharpe"]),
                dict(
                    strategy_id=strategy_id,
                    regime=regime,
                    realized_pnl=str(s["realized_pnl"]),
                    trade_count=s["trade_count"],
                    win_rate=str(s["win_rate"]),
                    sharpe=str(s["sharpe"]),
                ),
            )
        except Exception as exc:
            print(f"[weekly_attribution] DB insert failed for {strategy_id}/{regime}: {exc}")


def _build_summary_text(
    stats: dict[tuple[str, str], dict[str, Any]],
    period_start: date,
    period_end: date,
) -> str:
    """Format stats as a human-readable summary."""
    lines = [f"[주간 성과 귀속] {period_start} ~ {period_end}\n"]
    if not stats:
        lines.append("이번 주 거래 데이터 없음.")
        return "\n".join(lines)

    by_strategy: dict[str, list[str]] = {}
    for (strategy_id, regime), s in sorted(stats.items()):
        pnl_pct = s["realized_pnl"] * Decimal("100")
        sign = "+" if pnl_pct >= 0 else ""
        entry = f"{regime}: {sign}{pnl_pct:.2f}%"
        by_strategy.setdefault(strategy_id, []).append(entry)

    for strategy_id, entries in by_strategy.items():
        joined = ", ".join(entries)
        lines.append(f"• {strategy_id}: {joined}")

    return "\n".join(lines)


async def _generate_llm_summary(raw_summary: str) -> str:
    """Call Claude Sonnet to add analytical commentary."""
    if anthropic is None:
        return raw_summary

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return raw_summary

    try:
        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=512,
            messages=[
                {
                    "role": "user",
                    "content": (
                        "다음은 주간 전략 성과 귀속 데이터입니다. "
                        "간결한 한국어로 2~3문장 인사이트를 추가해 주세요:\n\n"
                        + raw_summary
                    ),
                }
            ],
        )
        insight = ""
        for block in message.content:
            if hasattr(block, "text"):
                insight = block.text.strip()
                break
        if insight:
            return raw_summary + "\n\n[AI 인사이트]\n" + insight
    except Exception as exc:
        print(f"[weekly_attribution] LLM call failed: {exc}")
    return raw_summary


async def _send_telegram(text: str) -> None:
    """Send message to all allowed Telegram users."""
    if Bot is None:
        print(text)
        return

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        print(text)
        return

    user_ids = _telegram_users()
    if not user_ids:
        print(text)
        return

    bot = Bot(token=token)
    for uid in user_ids:
        try:
            await bot.send_message(chat_id=uid, text=text[:4096])
        except Exception as exc:
            print(f"[weekly_attribution] Telegram send failed for {uid}: {exc}")


async def main() -> None:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("[weekly_attribution] DATABASE_URL not set, exiting.")
        return

    schema = _schema()
    now = datetime.now(timezone.utc)
    period_end = now.date()
    period_start = period_end - timedelta(days=7)

    conn = await asyncpg.connect(dsn)
    try:
        trades = await _fetch_weekly_trades(conn, schema)
        stats = _compute_stats(trades)
        await _upsert_attribution(conn, schema, stats, period_start, period_end)
    finally:
        await conn.close()

    raw_summary = _build_summary_text(stats, period_start, period_end)
    final_text = await _generate_llm_summary(raw_summary)
    await _send_telegram(final_text)
    print("[weekly_attribution] Done.")


if __name__ == "__main__":
    asyncio.run(main())
