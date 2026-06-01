"""Telegram bot entrypoint for control and candidate alerts."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import asyncpg
import yaml
from telegram import Bot, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

from core.operating_mode import approve_order_intent, get_pending_token, order_intent_status, reject_order_intent
from core.events.bus import RedisStreamBus
from core.events.schemas import EventType
from core.risk.limits import RiskManager


logger = logging.getLogger(__name__)


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _environment() -> str:
    return os.getenv("ENVIRONMENT", "paper").lower()


def _schema() -> str:
    return f"trading_{_environment()}"


def _redis_url() -> str:
    return os.getenv("REDIS_URL", "redis://redis:6379/0")


def _stream_prefix() -> str:
    return os.getenv("REDIS_STREAM_PREFIX", f"{_environment()}.events")


def _parse_allowed_user_ids() -> list[int]:
    raw = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "")
    return [int(item.strip()) for item in raw.split(",") if item.strip().isdigit()]


def _read_strategy_file_ids() -> set[str]:
    strategy_dir = "configs/strategies"
    strategy_ids: set[str] = set()
    for path in Path(strategy_dir).glob("*.yaml"):
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        strategy_id = payload.get("strategy_id")
        if strategy_id:
            strategy_ids.add(str(strategy_id).strip())
    return strategy_ids


def _parse_promote_args(args: list[str]) -> tuple[bool, str | None, bool]:
    if not args:
        return False, "사용법: /promote LIVE_AUTO --strategy STRATEGY_ID --confirm", False

    requested_mode = str(args[0]).strip().upper()
    if requested_mode != "LIVE_AUTO":
        return False, "현재는 LIVE_AUTO만 지정 가능합니다.", False

    strategy_id: str | None = None
    confirmed = False
    idx = 1
    while idx < len(args):
        item = str(args[idx]).strip()
        if item in {"--strategy", "-s"}:
            if idx + 1 >= len(args):
                return False, "--strategy 값이 필요합니다.", False
            strategy_id = str(args[idx + 1]).strip()
            idx += 2
            continue
        if item == "--confirm":
            confirmed = True
            idx += 1
            continue
        return False, f"알 수 없는 옵션: {item}", False

    if not strategy_id:
        return False, "--strategy STRATEGY_ID가 필요합니다.", False
    return True, strategy_id, confirmed


def _parse_disable_args(args: list[str]) -> tuple[bool, str | None]:
    if not args:
        return False, "사용법: /disable --strategy STRATEGY_ID"
    if args[0].startswith("--"):
        if args[0] in {"--strategy", "-s"}:
            if len(args) < 2:
                return False, "--strategy 값이 필요합니다."
            return True, str(args[1]).strip()
        return False, f"알 수 없는 옵션: {args[0]}"
    return True, str(args[0]).strip()


async def _strategy_exists(strategy_id: str) -> bool:
    return strategy_id in _read_strategy_file_ids()


async def _strategy_mode_map(dsn: str | None, strategy_id: str) -> tuple[str, bool]:
    default_mode = "PAPER"
    if not dsn:
        return default_mode, True

    conn = await asyncpg.connect(dsn)
    try:
        row = await conn.fetchrow(
            f"SELECT mode, COALESCE(is_active, TRUE) AS is_active FROM {_schema()}.strategy_modes WHERE strategy_id = $1",
            strategy_id,
        )
        if row is None:
            return default_mode, True
        return str(row["mode"] or default_mode), bool(row["is_active"])
    except Exception:
        return default_mode, True
    finally:
        await conn.close()


async def _set_strategy_mode(
    strategy_id: str,
    mode: str,
    is_active: bool,
    *,
    actor: str,
) -> None:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        return

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_schema()}.strategy_modes (
                strategy_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                updated_by TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            f"""
            INSERT INTO {_schema()}.strategy_modes (strategy_id, mode, is_active, updated_by)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (strategy_id) DO UPDATE
            SET mode = EXCLUDED.mode,
                is_active = EXCLUDED.is_active,
                updated_by = EXCLUDED.updated_by,
                updated_at = NOW()
            """,
            strategy_id,
            mode,
            is_active,
            actor,
        )
    finally:
        await conn.close()


async def _history_ok_for_strategy(strategy_id: str) -> bool:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        return True

    conn = await asyncpg.connect(dsn)
    try:
        history = await conn.fetchrow(
            f"""
            SELECT
                MIN(created_at) AS first_event,
                MAX(created_at) AS last_event,
                COUNT(*) AS hit_count
            FROM {_schema()}.audit_log
            WHERE target_type = 'strategy'
              AND target_id = $1
              AND action = '/promote'
              AND payload ->> 'mode' IN ('PAPER', 'LIVE_APPROVAL', 'PAPER_APPROVAL', 'LIVE_AUTO', 'LIVE')
            """,
            strategy_id,
        )
        if history is not None and history["first_event"] is not None and history["last_event"] is not None:
            span = history["last_event"] - history["first_event"]
            if span >= timedelta(days=14) and int(history["hit_count"] or 0) >= 2:
                return True

        span_rows = await conn.fetchrow(
            f"""
            SELECT
                MIN(created_at) AS first_fill,
                MAX(created_at) AS last_fill,
                COUNT(*) AS trade_count
            FROM {_schema()}.slippage_records
            WHERE strategy_id = $1
            """,
            strategy_id,
        )
        if span_rows is not None and span_rows["first_fill"] is not None and span_rows["last_fill"] is not None:
            span = span_rows["last_fill"] - span_rows["first_fill"]
            return span >= timedelta(days=14) and int(span_rows["trade_count"] or 0) > 0

        return False
    except Exception:
        return False
    finally:
        await conn.close()


async def _fetch_open_position_count(account_id: str) -> int:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        return 0

    conn = await asyncpg.connect(dsn)
    try:
        row = await conn.fetchrow(
            f"""
            SELECT COUNT(*) AS cnt
            FROM (
                SELECT DISTINCT ON (symbol) symbol, quantity
                FROM {_schema()}.position_snapshots
                WHERE account_id = $1
                ORDER BY symbol, snapshot_time DESC
            ) s
            WHERE quantity <> 0
            """,
            account_id,
        )
        if row is None:
            return 0
        return int(row["cnt"] or 0)
    except Exception:
        return 0
    finally:
        await conn.close()


def _allowed(update: Update) -> bool:
    if update.effective_user is None:
        return False
    return int(update.effective_user.id) in _parse_allowed_user_ids()


def _approval_mode() -> bool:
    return os.getenv("OPERATING_MODE", "READ_ONLY").upper() == "LIVE_APPROVAL"


def _format_candidate(payload: dict[str, Any]) -> str:
    code = str(payload.get("code") or payload.get("symbol", "")).strip()
    symbol_name = str(payload.get("symbol_name", "")).strip()
    strategy_id = str(payload.get("strategy_id", "")).strip()
    confidence = _to_decimal(payload.get("confidence", "0"))
    regime = str(payload.get("regime", "")).strip()
    news_summary = str(payload.get("news_summary", "")).strip()
    technical_summary = str(payload.get("technical_summary", "")).strip()

    risk = payload.get("risk") if isinstance(payload.get("risk"), dict) else {}
    position_pct = _to_decimal(risk.get("position_pct", risk.get("single_symbol_pct", "0")))
    spread_pct = _to_decimal(risk.get("spread_pct", "0"))
    stop_loss = _to_decimal(risk.get("stop_loss_pct", "0"))
    time_stop = risk.get("time_stop_minutes", "0")

    take_profit = risk.get("take_profit") if isinstance(risk.get("take_profit"), list) else []
    tps: list[Decimal] = []
    for item in take_profit:
        if isinstance(item, dict):
            tps.append(_to_decimal(item.get("pct", "0")))
    while len(tps) < 2:
        tps.append(Decimal("0"))

    price = _to_decimal(payload.get("price", payload.get("intended_price", "0")))
    quantity = _to_decimal(payload.get("quantity", payload.get("order_quantity", "0")))
    order_intent_id = str(payload.get("order_intent_id", "")).strip()

    lines = [
        f"[매수 후보] KR {code} {symbol_name}",
        f"전략: {strategy_id}",
        f"점수: {confidence}",
        f"국면: {regime}",
        f"뉴스: {news_summary}",
        f"차트: {technical_summary}",
        f"리스크: 단일종목 비중 {position_pct}%, 스프레드 {spread_pct}%",
        "",
        f"제안: 지정가 {price}원 × {quantity}주",
        f"손절: -{stop_loss}% / 익절: +{tps[0]}%, +{tps[1]}% / 시간손절: {time_stop}분",
        "",
    ]

    if _approval_mode():
        lines.append("상태: 수동 승인 대기 (만료 2분)")
        lines.append(f"명령어: /approve {order_intent_id}" if order_intent_id else "명령어: /approve <ORDER_INTENT_ID>")
    else:
        lines.append("상태: 모의 자동발주")
    return "\n".join(lines)


async def _record_audit(
    actor: str,
    command: str,
    target_type: str | None = None,
    target_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        return

    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            f"""
            INSERT INTO {_schema()}.audit_log (
                actor,
                action,
                target_type,
                target_id,
                payload
            ) VALUES ($1, $2, $3, $4, $5)
            """,
            actor,
            command,
            target_type,
            target_id,
            payload or {},
        )
    finally:
        await conn.close()


async def _fetch_recent_reports(symbol: str, limit: int = 5) -> list[dict[str, Any]]:
    redis_client = __import__("redis").asyncio.from_url(_redis_url(), decode_responses=True)
    stream = f"{_stream_prefix()}.{EventType.SIGNAL.value}"

    reports: list[dict[str, Any]] = []
    try:
        rows = await redis_client.xrevrange(stream, count=300)
        for _stream_name, messages in rows:
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
                if str(payload.get("code") or payload.get("symbol", "")) != symbol:
                    continue

                reports.append(
                    {
                        "strategy_id": str(payload.get("strategy_id", "")),
                        "confidence": str(payload.get("confidence", "0")),
                        "regime": str(payload.get("regime", "")),
                        "news_summary": str(payload.get("news_summary", "")),
                        "technical_summary": str(payload.get("technical_summary", "")),
                        "reason": str(payload.get("watch_reason", payload.get("reason", ""))),
                    }
                )

                if len(reports) >= limit:
                    return reports
    finally:
        await redis_client.aclose()

    return reports


async def cmd_daily_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    await _record_audit(actor=actor, command="/daily_report")
    if update.message is None:
        return
    await update.message.reply_text("일일 보고서: 구현 필요(요구 시 수동 호출).")


async def cmd_approve(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    if not _allowed(update):
        await _record_audit(actor=actor, command="/approve", target_type="order_intent", target_id=(context.args[0] if context.args else None))
        if update.message is not None:
            await update.message.reply_text("권한이 없습니다.")
        return

    if not context.args:
        await _record_audit(actor=actor, command="/approve", target_type="order_intent")
        if update.message is not None:
            await update.message.reply_text("사용법: /approve {ORDER_INTENT_ID}")
        return

    order_intent_id = str(context.args[0]).strip()
    token = await get_pending_token(order_intent_id, environment=_environment(), redis_url=_redis_url())
    if token is None:
        await _record_audit(
            actor=actor,
            command="/approve",
            target_type="order_intent",
            target_id=order_intent_id,
            payload={"status": "not_pending"},
        )
        if update.message is not None:
            await update.message.reply_text("현재 승인 대기 토큰이 없습니다.")
        return

    ok = await approve_order_intent(
        order_intent_id,
        token,
        environment=_environment(),
        redis_url=_redis_url(),
    )
    status = await order_intent_status(
        order_intent_id,
        environment=_environment(),
        redis_url=_redis_url(),
    )

    await _record_audit(
        actor=actor,
        command="/approve",
        target_type="order_intent",
        target_id=order_intent_id,
        payload={"status": status if ok else status},
    )

    if update.message is None:
        return

    if ok:
        await update.message.reply_text("주문이 승인되었습니다.")
        return

    await update.message.reply_text(f"승인되지 않음: {status or 'unknown'}")


async def cmd_reject(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    if not _allowed(update):
        await _record_audit(actor=actor, command="/reject", target_type="order_intent", target_id=(context.args[0] if context.args else None))
        if update.message is not None:
            await update.message.reply_text("권한이 없습니다.")
        return

    if not context.args:
        await _record_audit(actor=actor, command="/reject", target_type="order_intent")
        if update.message is not None:
            await update.message.reply_text("사용법: /reject {ORDER_INTENT_ID}")
        return

    order_intent_id = str(context.args[0]).strip()
    ok = await reject_order_intent(
        order_intent_id,
        environment=_environment(),
        redis_url=_redis_url(),
    )

    await _record_audit(
        actor=actor,
        command="/reject",
        target_type="order_intent",
        target_id=order_intent_id,
        payload={"status": "RISK_REJECTED" if ok else "FAILED"},
    )

    if update.message is None:
        return
    await update.message.reply_text("거부 처리되었습니다." if ok else "거부 처리 실패")


async def cmd_why(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    if not context.args:
        await _record_audit(actor=actor, command="/why", target_type="symbol")
        if update.message is not None:
            await update.message.reply_text("사용법: /why {SYMBOL}")
        return

    symbol = str(context.args[0]).strip()
    reports = await _fetch_recent_reports(symbol)
    await _record_audit(actor=actor, command="/why", target_type="symbol", target_id=symbol)

    if update.message is None:
        return
    if not reports:
        await update.message.reply_text(f"{symbol} 관련 watch reason가 없습니다.")
        return

    reason = reports[0].get("reason") or reports[0].get("news_summary") or reports[0].get("technical_summary")
    lines = [
        f"{symbol} 왜 보고 있나?",
        f"사유: {reason}",
        "최근 신호:",
    ]
    for report in reports:
        lines.append(f"- {report['strategy_id']} score={report['confidence']} regime={report['regime']}")

    await update.message.reply_text("\n".join(lines))


async def cmd_risk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    await _record_audit(actor=actor, command="/risk")

    manager = RiskManager()
    daily, weekly, _ = await manager.evaluate_loss_limits(
        os.getenv("ACCOUNT_ID", "default"),
        schema=_schema(),
    )
    lines = [
        "리스크 현황:",
        f"일일 손실: {daily.loss} / 제한 {daily.limit} (사용률 {daily.used_ratio * Decimal('100')}%)",
        f"주간 손실: {weekly.loss} / 제한 {weekly.limit} (사용률 {weekly.used_ratio * Decimal('100')}%)",
        f"실사용률: {manager.risk_usage_pct.get('daily_pct', Decimal('0'))}%",
    ]

    if update.message is not None:
        await update.message.reply_text("\n".join(lines))


async def cmd_halt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    if not _allowed(update):
        await _record_audit(actor=actor, command="/halt")
        if update.message is not None:
            await update.message.reply_text("권한이 없습니다.")
        return
    await _record_audit(actor=actor, command="/halt")
    if update.message is not None:
        await update.message.reply_text("HALT 실행 요청 접수")


async def cmd_promote(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    if not _allowed(update):
        await _record_audit(actor=actor, command="/promote")
        if update.message is not None:
            await update.message.reply_text("권한이 없습니다.")
        return
    await _record_audit(actor=actor, command="/promote")
    if update.message is not None:
        await update.message.reply_text("/promote 명령을 수신했습니다.")


async def cmd_resume_live(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    if not _allowed(update):
        await _record_audit(actor=actor, command="/resume_live")
        if update.message is not None:
            await update.message.reply_text("권한이 없습니다.")
        return
    await _record_audit(actor=actor, command="/resume_live")
    if update.message is not None:
        await update.message.reply_text("resume_live 명령을 수신했습니다.")


async def cmd_disable(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    if not _allowed(update):
        await _record_audit(actor=actor, command="/disable")
        if update.message is not None:
            await update.message.reply_text("권한이 없습니다.")
        return
    await _record_audit(actor=actor, command="/disable")
    if update.message is not None:
        await update.message.reply_text("disable 명령을 수신했습니다.")


async def _candidate_listener() -> None:
    user_ids = _parse_allowed_user_ids()
    if not user_ids:
        return

    bus = RedisStreamBus(redis_url=_redis_url(), stream_prefix=_stream_prefix())

    async for event in bus.subscribe(EventType.SIGNAL):
        payload = event.payload if isinstance(event, object) else {}
        if not isinstance(payload, dict):
            continue
        if payload.get("event_type") != "news_candidate":
            continue
        text = _format_candidate(payload)
        for user_id in user_ids:
            try:
                await Bot(token=os.getenv("TELEGRAM_BOT_TOKEN")).send_message(chat_id=user_id, text=text)
            except Exception:
                continue


def _actor(update: Update) -> str:
    if update.effective_user is None:
        return "unknown"
    return str(update.effective_user.id)


async def _runner() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    normalized_token = str(token).strip() if token is not None else ""
    if normalized_token.lower() in {"", "none", "null"}:
        logger.warning("TELEGRAM_BOT_TOKEN is not configured. Telegram bot startup is skipped and process stays alive.")
        while True:
            await asyncio.sleep(3600)
        return

    app = ApplicationBuilder().token(normalized_token).build()
    app.add_handler(CommandHandler("daily_report", cmd_daily_report))
    app.add_handler(CommandHandler("approve", cmd_approve))
    app.add_handler(CommandHandler("reject", cmd_reject))
    app.add_handler(CommandHandler("why", cmd_why))
    app.add_handler(CommandHandler("risk", cmd_risk))
    app.add_handler(CommandHandler("halt", cmd_halt))
    app.add_handler(CommandHandler("promote", cmd_promote))
    app.add_handler(CommandHandler("resume_live", cmd_resume_live))
    app.add_handler(CommandHandler("disable", cmd_disable))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    await _candidate_listener()

    await app.updater.stop()
    await app.stop()
    await app.shutdown()


if __name__ == "__main__":
    asyncio.run(_runner())
