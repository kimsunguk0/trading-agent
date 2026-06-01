"""Scheduled morning/evening briefings generated with Claude Sonnet."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import asyncpg
import anthropic
import yaml

from telegram import Bot


def _today_kst() -> datetime:
    return datetime.now(ZoneInfo("Asia/Seoul"))


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _config_section() -> dict[str, Any]:
    cfg_path = Path("configs/llm/routing.yaml")
    if not cfg_path.exists():
        return {}

    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        return {}
    section = raw.get("daily_briefing")
    return section if isinstance(section, dict) else {}


def _model_name() -> str:
    section = _config_section()
    model = section.get("primary")
    if isinstance(model, str) and model:
        return model
    return "claude-sonnet"


def _schema() -> str:
    return f"trading_{__import__('os').getenv('ENVIRONMENT', 'paper')}"


def _account_id() -> str:
    return __import__("os").getenv("ACCOUNT_ID", "default")


def _allowed_users() -> list[int]:
    env = __import__("os").getenv("TELEGRAM_ALLOWED_USER_IDS", "")
    return [int(item.strip()) for item in env.split(",") if item.strip().isdigit()]


@dataclass
class _MacroEvent:
    event_type: str
    time: str
    summary: str


async def _fetch_macro_issues() -> list[_MacroEvent]:
    dsn = __import__("os").getenv("DATABASE_URL")
    if not dsn:
        return []

    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            f"""
            SELECT event_type, event_time, evidence_json
            FROM {_schema()}.normalized_events
            WHERE event_time >= NOW() - INTERVAL '24 hours'
            ORDER BY event_time DESC
            LIMIT 12
            """
        )
        out: list[_MacroEvent] = []
        for row in rows:
            evidence = row["evidence_json"]
            if isinstance(evidence, dict):
                summary = str(evidence.get("title") or evidence.get("summary") or "")
            else:
                summary = str(evidence or "")
            out.append(
                _MacroEvent(
                    event_type=str(row["event_type"]),
                    time=str(row["event_time"]),
                    summary=summary,
                )
            )
        return out
    finally:
        await conn.close()


async def _fetch_strategy_status() -> dict[str, Any]:
    dsn = __import__("os").getenv("DATABASE_URL")
    if not dsn:
        return {}

    conn = await asyncpg.connect(dsn)
    try:
        row = await conn.fetchrow(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE stage = 'limits' AND NOT passed) AS denied_by_risk,
                COUNT(*) FILTER (WHERE stage = 'compliance') AS denied_by_compliance,
                COUNT(*) AS total_events
            FROM {_schema()}.risk_events
            WHERE created_at >= NOW() - INTERVAL '1 day'
            """
        )
        if row is None:
            return {}
        return {
            "total_events": int(row["total_events"] or 0),
            "denied_by_risk": int(row["denied_by_risk"] or 0),
            "denied_by_compliance": int(row["denied_by_compliance"] or 0),
        }
    finally:
        await conn.close()


async def _fetch_evening_stats() -> dict[str, Any]:
    dsn = __import__("os").getenv("DATABASE_URL")
    if not dsn:
        return {"pnl": Decimal("0"), "avg_slippage": Decimal("0"), "watchlist": []}

    conn = await asyncpg.connect(dsn)
    try:
        pnl_rows = await conn.fetchrow(
            f"""
            SELECT
                COALESCE(
                    (SELECT cash_balance FROM {_schema()}.cash_snapshots WHERE account_id = $1 ORDER BY snapshot_time ASC LIMIT 1),
                    0
                ) AS start_cash,
                COALESCE(
                    (SELECT cash_balance FROM {_schema()}.cash_snapshots WHERE account_id = $1 ORDER BY snapshot_time DESC LIMIT 1),
                    0
                ) AS end_cash
            """,
            _account_id(),
        )
        slippage_row = await conn.fetchrow(
            f"""
            SELECT COALESCE(AVG(ABS(slippage_pct)), 0) AS avg_slippage
            FROM {_schema()}.slippage_records
            WHERE filled_at >= NOW() - INTERVAL '1 day'
            """
        )
        watch_rows = await conn.fetch(
            f"""
            SELECT DISTINCT ON (symbol) symbol
            FROM {_schema()}.position_snapshots
            WHERE account_id = $1
            ORDER BY symbol, snapshot_time DESC
            LIMIT 12
            """,
            _account_id(),
        )

        pnl = (pnl_rows["end_cash"] if pnl_rows else Decimal("0")) - (pnl_rows["start_cash"] if pnl_rows else Decimal("0"))
        return {
            "pnl": _to_decimal(pnl),
            "avg_slippage": _to_decimal(slippage_row["avg_slippage"] if slippage_row else Decimal("0")),
            "watchlist": [str(item["symbol"]) for item in watch_rows],
        }
    finally:
        await conn.close()


async def _compose_with_claude(context: str, payload: dict[str, Any]) -> str:
    api_key = __import__("os").getenv("ANTHROPIC_API_KEY")
    if not api_key:
        if context == "morning":
            return "[브리핑] 수집 가능한 데이터가 부족합니다. 매크로 이벤트/전략 상태만 확인 가능합니다."
        return "[브리핑] 수집 가능한 데이터가 부족합니다."

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=_model_name(),
        max_tokens=1200,
        temperature=0.3,
        system="한국어로 간결하고 실전적인 브리핑을 생성한다.",
        messages=[
            {
                "role": "user",
                "content": (
                    "요청: "
                    + context
                    + " 형식의 브리핑\n"
                    + f"데이터:\n{payload}\n"
                    + "출력은 실행 가능한 한글 요약형 문장으로만 작성.\n"
                ),
            }
        ],
    )

    first = response.content[0] if response.content else None
    if first is None:
        return "브리핑 생성 실패"

    if hasattr(first, "text"):
        return str(first.text)

    return "브리핑 생성 실패"


async def _compose_brieing(context: str) -> str:
    if context == "morning":
        macro = await _fetch_macro_issues()
        strategy_status = await _fetch_strategy_status()
        payload = {
            "time": _today_kst().strftime("%Y-%m-%d %H:%M"),
            "macro_calendar": ["%s %s %s" % (item.time, item.event_type, item.summary) for item in macro[:6]],
            "strategy_status": strategy_status,
            "requirements": "macro calendar, key issues, strategy health",
        }
    else:
        evening = await _fetch_evening_stats()
        payload = {
            "time": _today_kst().strftime("%Y-%m-%d %H:%M"),
            "pnl": str(evening["pnl"]),
            "avg_slippage": str(evening["avg_slippage"]),
            "watchlist_tomorrow": evening["watchlist"],
            "requirements": "P/L, slippage, watchlist",
        }

    return await _compose_with_claude(context, payload)


def _need_send(last: datetime | None, now: datetime, target_hour: int, target_minute: int) -> bool:
    if now.hour != target_hour or now.minute != target_minute:
        return False
    return last is None or last.date() != now.date() or last.hour != now.hour or last.minute != now.minute


async def _send(bot: Bot, text: str) -> None:
    users = _allowed_users()
    for user_id in users:
        await bot.send_message(chat_id=user_id, text=text)


async def main() -> None:
    token = __import__("os").getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    bot = Bot(token=token)
    last_morning: datetime | None = None
    last_evening: datetime | None = None

    try:
        while True:
            now = _today_kst()
            if _need_send(last_morning, now, 8, 30):
                morning = await _compose_brieing("morning")
                await _send(bot, morning)
                last_morning = now

            if _need_send(last_evening, now, 15, 40):
                evening = await _compose_brieing("evening")
                await _send(bot, evening)
                last_evening = now

            await asyncio.sleep(20)
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
