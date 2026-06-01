"""Natural-language control shim for Telegram commands."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import anthropic
from telegram import Message, Update
from telegram.ext import ApplicationBuilder, MessageHandler, ContextTypes, filters

from core.events.schemas import EventType
from core.operating_mode import approve_order_intent, get_pending_token, reject_order_intent
from core.risk.limits import RiskManager


import yaml
from pathlib import Path
import redis.asyncio as redis


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _allowed(update: Update) -> bool:
    if update.effective_user is None:
        return False
    raw = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "")
    allowed = {int(item.strip()) for item in raw.split(",") if item.strip().isdigit()}
    return int(update.effective_user.id) in allowed


def _environment() -> str:
    return os.getenv("ENVIRONMENT", "paper").lower()


def _redis_url() -> str:
    return os.getenv("REDIS_URL", "redis://localhost:6379/0")


def _schema() -> str:
    return f"trading_{_environment()}"


def _actor(update: Update) -> str:
    if update.effective_user is None:
        return "unknown"
    return str(update.effective_user.id)


def _llm_settings() -> dict[str, str]:
    cfg_path = Path(os.getenv("LLM_ROUTING_PATH", "configs/llm/routing.yaml"))
    if cfg_path.exists():
        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        if isinstance(raw, dict):
            convo = raw.get("conversational_control", {})
            if isinstance(convo, dict):
                return {
                    "model": str(convo.get("primary", "claude-sonnet")),
                }
    return {"model": "claude-sonnet"}


@dataclass
class ParsedIntent:
    action: str
    symbol: str | None = None
    order_intent_id: str | None = None


def _fallback_intent(text: str) -> ParsedIntent:
    lowered = text.lower()
    if "왜" in text or "why" in lowered:
        symbol = _extract_symbol(text)
        return ParsedIntent(action="why", symbol=symbol)
    if "리스크" in text or "risk" in lowered:
        return ParsedIntent(action="risk")
    if "승인" in text or "approve" in lowered:
        oid = _extract_symbol(text, allow_alpha=False) or ""
        return ParsedIntent(action="approve", order_intent_id=oid)
    if "거절" in text or "reject" in lowered:
        oid = _extract_symbol(text, allow_alpha=False) or ""
        return ParsedIntent(action="reject", order_intent_id=oid)
    return ParsedIntent(action="noop")


def _extract_symbol(text: str, *, allow_alpha: bool = True) -> str | None:
    import re

    candidate = re.search(r"\b\d{5,6}\b", text)
    if candidate:
        return candidate.group(0)

    if not allow_alpha:
        return None

    if "삼성전자" in text:
        return "005930"
    if "lg" in text.lower():
        return "066570"
    if "에스케이" in text or "000660" in text:
        return "000660"
    return None


async def _record_audit(actor: str, command: str, target_type: str | None = None, target_id: str | None = None) -> None:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        return

    conn = await __import__("asyncpg").connect(dsn)
    try:
        await conn.execute(
            f"""
            INSERT INTO {_schema()}.audit_log (actor, action, target_type, target_id)
            VALUES ($1, $2, $3, $4)
            """,
            actor,
            command,
            target_type,
            target_id,
        )
    finally:
        await conn.close()


async def _fetch_recent_reports(symbol: str, limit: int = 5) -> list[dict[str, Any]]:
    redis_client = redis.from_url(_redis_url(), decode_responses=True)
    stream = f"{os.getenv('REDIS_STREAM_PREFIX', f'{_environment()}.events')}.{EventType.SIGNAL.value}"
    out: list[dict[str, Any]] = []

    try:
        records = await redis_client.xrevrange(stream, count=300)
        for _stream, messages in records:
            for _message_id, fields in messages:
                raw = fields.get("payload") if isinstance(fields, dict) else None
                if not isinstance(raw, str):
                    continue
                try:
                    payload = json.loads(raw)
                except Exception:
                    continue
                if payload.get("event_type") != "news_candidate":
                    continue
                if str(payload.get("code", payload.get("symbol", ""))) != symbol:
                    continue
                out.append(
                    {
                        "symbol": str(payload.get("code", symbol)),
                        "strategy_id": str(payload.get("strategy_id", "")),
                        "confidence": str(payload.get("confidence", "")),
                        "regime": str(payload.get("regime", "")),
                        "news_summary": str(payload.get("news_summary", "")),
                        "technical_summary": str(payload.get("technical_summary", "")),
                        "watch_reason": str(payload.get("watch_reason", payload.get("reason", ""))),
                    }
                )
                if len(out) >= limit:
                    return out
    finally:
        await redis_client.aclose()

    return out


async def parse_nl_intent(text: str) -> ParsedIntent:
    settings = _llm_settings()
    api_key = os.getenv("ANTHROPIC_API_KEY")

    if api_key:
        prompt = """
사용자 입력을 아래 JSON으로만 반환하라.
{"action":"why|risk|approve|reject|noop","symbol":"...","order_intent_id":"..."}
왜/why는 symbol이 필수, approve/reject는 order_intent_id가 필수.
"""
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=settings["model"],
            max_tokens=120,
            system="자연어를 주어진 스키마(JSON)로 변환", 
            messages=[{"role": "user", "content": prompt + f"\n입력: {text}"}],
        )
        block = resp.content[0] if resp.content else None
        if block is not None and hasattr(block, "text"):
            body = str(block.text).strip()
            try:
                parsed = json.loads(body)
                action = str(parsed.get("action", "noop"))
                if action in {"why", "risk", "approve", "reject", "noop"}:
                    return ParsedIntent(
                        action=action,
                        symbol=parsed.get("symbol"),
                        order_intent_id=parsed.get("order_intent_id"),
                    )
            except Exception:
                pass

    return _fallback_intent(text)


async def _execute(update: Update, intent: ParsedIntent) -> str:
    actor_id = _actor(update)
    if intent.action in {"why", "approve", "reject", "risk"}:
        await _record_audit(actor=actor_id, command=f"/{intent.action}", target_id=intent.order_intent_id or intent.symbol)

    if intent.action == "noop":
        return "무엇을 원하시는지 알 수 없어요. 예) /approve <ID>, /reject <ID>, /why 005930"

    if intent.action == "risk":
        manager = RiskManager()
        daily, weekly, _ = await manager.evaluate_loss_limits(
            os.getenv("ACCOUNT_ID", "default"),
            schema=f"trading{ '_' + _environment() }",
        )
        return (
            f"일일 손실 {daily.loss}/{daily.limit} (사용률 {daily.used_ratio * Decimal('100'):.2f}%)\n"
            f"주간 손실 {weekly.loss}/{weekly.limit} (사용률 {weekly.used_ratio * Decimal('100'):.2f}%)"
        )

    if intent.action == "why":
        symbol = (intent.symbol or "").strip()
        if not symbol:
            return "심볼을 입력해 주세요. 예: /why 005930"
        reports = await _fetch_recent_reports(symbol)
        if not reports:
            return f"{symbol} 최근 감시 사유가 없습니다."

        latest = reports[0]
        return (
            f"{symbol} 왜 보고 있는지\n"
            f"- 전략: {latest['strategy_id']}\n"
            f"- 신뢰도: {latest['confidence']}\n"
            f"- 국면: {latest['regime']}\n"
            f"- 이유: {latest.get('watch_reason') or latest['news_summary']}"
        )

    if intent.action == "approve":
        order_intent_id = (intent.order_intent_id or "").strip()
        if not order_intent_id:
            return "승인할 주문 ID를 입력하세요."
        token = await get_pending_token(order_intent_id, environment=_environment(), redis_url=_redis_url())
        if token is None:
            return "대기 중 승인 토큰이 없습니다."
        ok = await approve_order_intent(
            order_intent_id,
            token,
            environment=_environment(),
            redis_url=_redis_url(),
        )
        return "승인 처리됨" if ok else "승인 실패"

    order_intent_id = (intent.order_intent_id or "").strip()
    if not order_intent_id:
        return "거부할 주문 ID를 입력하세요."
    ok = await reject_order_intent(
        order_intent_id,
        environment=_environment(),
        redis_url=_redis_url(),
    )
    return "거부 처리됨" if ok else "거부 처리 실패"


async def _handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _allowed(update):
        if update.message is not None:
            await update.message.reply_text("권한이 없습니다.")
        return

    if update.message is None or update.message.text is None:
        return

    intent = await parse_nl_intent(update.message.text)
    text = await _execute(update, intent)
    await update.message.reply_text(text)


async def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    app = ApplicationBuilder().token(token).build()
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, _handle_message))
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    try:
        while True:
            await asyncio_sleep(120)
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


async def asyncio_sleep(seconds: int) -> None:
    import asyncio

    await asyncio.sleep(seconds)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
