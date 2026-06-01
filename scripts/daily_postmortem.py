"""Generate daily post-mortem notes from today's losing trades."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import asyncpg
import anthropic
import yaml


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _environment() -> str:
    import os

    return os.getenv("ENVIRONMENT", "paper").lower()


def _schema() -> str:
    return f"trading_{_environment()}"


def _account() -> str:
    return __import__("os").getenv("ACCOUNT_ID", "default")


def _today_utc() -> datetime:
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def _routing_model() -> str:
    cfg_path = Path("configs/llm/routing.yaml")
    if not cfg_path.exists():
        return "claude-sonnet"

    payload = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return "claude-sonnet"

    section = payload.get("daily_post_mortem", payload.get("daily_briefing"))
    if not isinstance(section, dict):
        return "claude-sonnet"

    model = section.get("primary")
    if isinstance(model, str) and model:
        return model
    return "claude-sonnet"


async def _snapshot_realized_delta(
    dsn: str,
    schema: str,
    account_id: str,
    symbol: str,
    at: datetime,
) -> Decimal:
    conn = await asyncpg.connect(dsn)
    try:
        before = await conn.fetchrow(
            f"""
            SELECT COALESCE(realized_pnl, 0) AS realized_pnl
            FROM {schema}.position_snapshots
            WHERE account_id = $1
              AND symbol = $2
              AND snapshot_time <= $3
            ORDER BY snapshot_time DESC
            LIMIT 1
            """,
            account_id,
            symbol,
            at,
        )

        after = await conn.fetchrow(
            f"""
            SELECT COALESCE(realized_pnl, 0) AS realized_pnl
            FROM {schema}.position_snapshots
            WHERE account_id = $1
              AND symbol = $2
              AND snapshot_time >= $3
            ORDER BY snapshot_time ASC
            LIMIT 1
            """,
            account_id,
            symbol,
            at,
        )

        if before is None or after is None:
            return Decimal("0")
        return _to_decimal(after["realized_pnl"]) - _to_decimal(before["realized_pnl"])
    finally:
        await conn.close()


async def _fetch_losing_trades(dsn: str, schema: str, account_id: str) -> list[dict[str, Any]]:
    start = _today_utc()
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            f"""
            SELECT f.order_intent_id, f.quantity, f.price, f.filled_at, oi.symbol
            FROM {schema}.fills f
            JOIN {schema}.order_intents oi ON oi.order_intent_id = f.order_intent_id
            WHERE oi.account_id = $1
              AND f.filled_at >= $2
            ORDER BY f.filled_at DESC
            LIMIT 300
            """,
            account_id,
            start,
        )
    finally:
        await conn.close()

    losing: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row["symbol"])
        realized_delta = await _snapshot_realized_delta(dsn, schema, account_id, symbol, row["filled_at"])
        if realized_delta >= Decimal("0"):
            continue

        losing.append(
            {
                "order_intent_id": str(row["order_intent_id"]),
                "symbol": symbol,
                "quantity": _to_decimal(row["quantity"]),
                "filled_price": _to_decimal(row["price"]),
                "filled_at": row["filled_at"],
                "realized_delta": realized_delta,
            }
        )

    return losing


async def _fetch_signal_context(dsn: str, schema: str, order_intent_id: str) -> list[dict[str, Any]]:
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            f"""
            SELECT payload
            FROM {schema}.audit_log
            WHERE target_type = 'order_intent'
              AND target_id = $1
              AND payload IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 5
            """,
            order_intent_id,
        )
        return [dict(row["payload"]) for row in rows if isinstance(row["payload"], dict)]
    finally:
        await conn.close()


def _build_fallback(trades: list[dict[str, Any]]) -> str:
    today = _today_utc().date().isoformat()
    if not trades:
        return f"[{today}] 오늘 손실 확정 트레이드가 없습니다."

    lines = [f"[{today}] 일일 사후분석"]
    lines.append(f"손실 후보: {len(trades)}")
    for trade in trades[:15]:
        lines.append(
            f"- {trade['order_intent_id']} {trade['symbol']} {trade['quantity']}주 @ {trade['filled_price']} "
            f"(delta {trade['realized_delta']})"
        )
    return "\n".join(lines)


async def _compose_narrative(
    trades: list[dict[str, Any]],
    context_map: dict[str, list[dict[str, Any]]],
) -> str:
    api_key = __import__("os").getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return _build_fallback(trades)

    prompt = (
        "너는 트레이딩 사후검토 전문가다.\n"
        "오늘 손실 거래를 분석해 어떤 신호를 무시했어야 했는지 제안한다.\n"
        "lossing_trades와 signal_context로 ignore_candidates/mistakes/lessons를 JSON 형태로 3개씩 정리해라."
    )

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=_routing_model(),
        max_tokens=1200,
        temperature=0.3,
        system="한국어로 실무 중심의 사후분석을 생성한다.",
        messages=[
            {
                "role": "user",
                "content": f"{prompt}\n\nlosing_trades={trades}\n\nsignal_context={context_map}",
            }
        ],
    )

    if not response or not response.content:
        return _build_fallback(trades)

    first = response.content[0]
    if hasattr(first, "text"):
        return str(first.text)

    return _build_fallback(trades)


async def _persist_journal_entry(
    dsn: str,
    schema: str,
    account_id: str,
    content: str,
) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.journal_entries (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                account_id TEXT,
                entry_type TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
        )
        await conn.execute(
            f"""
            INSERT INTO {schema}.journal_entries (account_id, entry_type, content)
            VALUES ($1, $2, $3)
            """,
            account_id,
            "daily_post_mortem",
            content,
        )
    finally:
        await conn.close()


async def main() -> None:
    dsn = __import__("os").getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is required")

    schema = _schema()
    account_id = _account()

    trades = await _fetch_losing_trades(dsn, schema, account_id)
    context: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        order_intent_id = str(trade["order_intent_id"])
        context[order_intent_id] = await _fetch_signal_context(dsn, schema, order_intent_id)

    narrative = await _compose_narrative(trades, context)
    await _persist_journal_entry(dsn, schema, account_id, narrative)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
