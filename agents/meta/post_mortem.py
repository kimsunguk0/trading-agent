"""Daily post-mortem analysis for losing trades."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg
import anthropic
import redis.asyncio as redis


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _schema() -> str:
    return f"trading_{os.getenv('ENVIRONMENT', 'paper')}"


def _redis_url() -> str:
    return os.getenv("REDIS_URL", "redis://localhost:6379/0")


def _to_dict(message: Any) -> str:
    if hasattr(message, "text"):
        return str(message.text)
    return str(message)


def _parse_lessons(response: str) -> tuple[str, list[str]]:
    text = response.strip()
    narrative = text
    lessons: list[str] = []
    lowered = text.lower()
    if "lesson" in lowered:
        parts = text.split("\n")
        capture = False
        for part in parts:
            if "lesson" in part.lower() and ":" in part:
                capture = True
                payload = part.split(":", 1)[1].strip()
                if payload:
                    lessons.extend([item.strip() for item in payload.split(",") if item.strip()])
                continue
            if capture and part.strip().startswith("-"):
                lessons.append(part.lstrip("- ").strip())
            elif capture and part.strip() and not part.startswith(" "):
                # stop capture when next section starts
                capture = False

    if not lessons and "lessons" not in lowered:
        if "ignore" in lowered:
            sections = [line.strip() for line in parts if "ignore" in line.lower()]
            lessons.extend(sections[:3])

    return narrative, lessons[:5]


class PostMortem:
    def __init__(self, *, dsn: str | None = None) -> None:
        self.dsn = dsn or os.getenv("DATABASE_URL")
        self.schema = _schema()

    async def _fetch_losing_trades(self, conn: asyncpg.Connection) -> list[dict[str, Any]]:
        query_variants = (
            f"""
            SELECT
                order_intent_id,
                strategy_id,
                symbol_code,
                exit_at,
                realized_pnl,
                pnl_pct,
                regime_at_entry
            FROM {self.schema}.closed_trades
            WHERE exit_at >= date_trunc('day', NOW())
              AND realized_pnl < 0
            ORDER BY exit_at DESC
            """,
            f"""
            SELECT
                order_intent_id,
                strategy_id,
                symbol_code,
                filled_at AS exit_at,
                -slippage_pct AS realized_pnl,
                -slippage_pct AS pnl_pct,
                'UNKNOWN' AS regime_at_entry
            FROM {self.schema}.slippage_records
            WHERE filled_at >= date_trunc('day', NOW())
            ORDER BY filled_at DESC
            """,
        )

        rows = []
        for query in query_variants:
            try:
                rows = await conn.fetch(query)
                break
            except Exception:
                rows = []
                continue

        return [
            {
                "trade_id": str(row["order_intent_id"] or ""),
                "strategy_id": str(row["strategy_id"] or "UNKNOWN"),
                "symbol_code": str(row["symbol_code"] or ""),
                "exit_at": row["exit_at"],
                "realized_pnl": _to_decimal(row["realized_pnl"]),
                "pnl_pct": _to_decimal(row["pnl_pct"]),
                "regime_at_entry": str(row["regime_at_entry"] or "UNKNOWN"),
            }
            for row in rows
            if _to_decimal(row["realized_pnl"]) < Decimal("0")
        ]

    async def _build_prompt(self, trades: list[dict[str, Any]]) -> str:
        detail = []
        for trade in trades:
            detail.append(
                {
                    "trade_id": trade["trade_id"],
                    "strategy_id": trade["strategy_id"],
                    "symbol_code": trade["symbol_code"],
                    "exit_at": str(trade["exit_at"]),
                    "realized_pnl": str(trade["realized_pnl"]),
                    "pnl_pct": str(trade["pnl_pct"]),
                    "regime": trade["regime_at_entry"],
                }
            )

        return (
            "오늘 손실 거래를 기반으로 어떤 시그널을 무시했어야 했는지 정리해줘.\n"
            "다음 JSON은 거래 상세다.\n"
            f"{json.dumps(detail, ensure_ascii=False)}\n"
            "결과는 narrative(실전 요약)와 lessons(무시해야 할 시그널)으로만 JSON으로 반환."
        )

    async def _set_context(self, lessons: list[str]) -> None:
        redis_client = redis.from_url(_redis_url(), decode_responses=True)
        try:
            await redis_client.set("shared_context:recent_lessons", json.dumps(lessons, ensure_ascii=False))
        finally:
            await redis_client.aclose()

    async def _insert_journal(self, conn: asyncpg.Connection, trade: dict[str, Any], narrative: str, lessons: list[str]) -> None:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.journal_entries (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                trade_id UUID,
                strategy_id TEXT,
                symbol_market TEXT,
                symbol_code TEXT,
                entry_at TIMESTAMPTZ,
                exit_at TIMESTAMPTZ,
                pnl NUMERIC(20,8),
                pnl_pct NUMERIC(10,6),
                regime_at_entry TEXT,
                signals_used JSONB,
                news_refs JSONB,
                narrative TEXT,
                lessons TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )

        await conn.execute(
            f"""
            INSERT INTO {self.schema}.journal_entries (
                trade_id,
                strategy_id,
                symbol_market,
                symbol_code,
                exit_at,
                pnl,
                pnl_pct,
                regime_at_entry,
                signals_used,
                news_refs,
                narrative,
                lessons
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
            trade["trade_id"] or None,
            trade["strategy_id"],
            "KR",
            trade["symbol_code"],
            trade["exit_at"],
            str(trade["realized_pnl"]),
            str(trade["pnl_pct"]),
            trade["regime_at_entry"],
            json.dumps({}, ensure_ascii=False),
            json.dumps([], ensure_ascii=False),
            narrative,
            ", ".join(lessons),
        )

    async def daily_analysis(self) -> None:
        if not self.dsn:
            return

        conn = await asyncpg.connect(self.dsn)
        try:
            trades = await self._fetch_losing_trades(conn)
            if not trades:
                return

            prompt = await self._build_prompt(trades)
            api_key = os.getenv("ANTHROPIC_API_KEY")
            if api_key:
                client = anthropic.Anthropic(api_key=api_key)
                response = client.messages.create(
                    model="claude-sonnet-4-5",
                    max_tokens=1200,
                    temperature=0.25,
                    system="한국어로 실전형 분석 텍스트를 작성한다.",
                    messages=[{"role": "user", "content": prompt}],
                )
                text = _to_dict(response.content[0]) if response.content else ""
            else:
                text = "No ANTHROPIC_API_KEY configured."

            narrative, lessons = _parse_lessons(text)
            await self._set_context(lessons)
            for trade in trades:
                await self._insert_journal(conn, trade, narrative, lessons)
        finally:
            await conn.close()
