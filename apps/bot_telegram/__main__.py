"""Telegram bot entrypoint for control and candidate alerts."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

try:
    import asyncpg
except Exception:  # pragma: no cover
    asyncpg = None

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None

try:
    from telegram import Bot, Update
    from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
except Exception:  # pragma: no cover - package-less local smoke imports
    class Bot:  # type: ignore[no-redef]
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        async def send_message(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    class Update:  # type: ignore[no-redef]
        pass

    class _NoopUpdater:
        async def start_polling(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    class _NoopApplication:
        updater = _NoopUpdater()

        def add_handler(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        async def initialize(self) -> None:
            return None

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def shutdown(self) -> None:
            return None

    class ApplicationBuilder:  # type: ignore[no-redef]
        def token(self, *_args: Any, **_kwargs: Any) -> "ApplicationBuilder":
            return self

        def build(self) -> _NoopApplication:
            return _NoopApplication()

    class CommandHandler:  # type: ignore[no-redef]
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

    class ContextTypes:  # type: ignore[no-redef]
        DEFAULT_TYPE = object

from core.events.schemas import EventType
from core.operating_mode import (
    OperatingMode,
    approve_order_intent,
    get_pending_token,
    normalize_mode,
    order_intent_status,
    reject_order_intent,
)
from core.risk.limits import RiskManager
from core.system_state import SystemState, SystemStateMachine
from core.trading_controls import set_trading_control


logger = logging.getLogger(__name__)

_FALLBACK_STRATEGY_MODES: dict[str, tuple[str, bool]] = {}
_FALLBACK_RUNTIME_MODES: dict[str, str] = {}


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _fmt_decimal(value: Any) -> str:
    try:
        number = _to_decimal(value)
    except Exception:
        return str(value)
    return f"{number.normalize():f}"


def _environment() -> str:
    return os.getenv("ENVIRONMENT", "paper").lower()


def _schema() -> str:
    return f"trading_{_environment()}"


def _dsn() -> str | None:
    raw = os.getenv("DATABASE_URL")
    if raw is None or not raw.strip():
        return None
    return raw.strip()


def _redis_url() -> str:
    return os.getenv("REDIS_URL", "redis://redis:6379/0")


def _stream_prefix() -> str:
    return os.getenv("REDIS_STREAM_PREFIX", f"{_environment()}.events")


def _account_id() -> str:
    return os.getenv("ACCOUNT_ID", "default")


def _parse_allowed_user_ids() -> list[int]:
    raw = os.getenv("TELEGRAM_ALLOWED_USER_IDS", "")
    return [int(item.strip()) for item in raw.split(",") if item.strip().isdigit()]


def _read_strategy_file_ids() -> set[str]:
    if yaml is None:
        return set()
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


def _actor(update: Update) -> str:
    if getattr(update, "effective_user", None) is None:
        return "unknown"
    return str(update.effective_user.id)


def _allowed(update: Update) -> bool:
    if getattr(update, "effective_user", None) is None:
        return False
    return int(update.effective_user.id) in _parse_allowed_user_ids()


async def _reply(update: Update, text: str) -> None:
    if getattr(update, "message", None) is None:
        return
    await update.message.reply_text(text)


async def _db_connect() -> Any | None:
    if asyncpg is None:
        return None
    dsn = _dsn()
    if not dsn:
        return None
    try:
        return await asyncpg.connect(dsn)
    except Exception as exc:
        logger.warning("Postgres unavailable for Telegram command: %s", exc)
        return None


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        return row[key]
    except Exception:
        pass
    getter = getattr(row, "get", None)
    if callable(getter):
        return getter(key, default)
    return default


def _redis_client() -> Any | None:
    try:
        redis_asyncio = __import__("redis.asyncio", fromlist=["from_url"])
    except Exception:
        return None
    try:
        return redis_asyncio.from_url(_redis_url(), decode_responses=True)
    except Exception as exc:
        logger.warning("Redis unavailable for Telegram command: %s", exc)
        return None


async def _close_client(client: Any) -> None:
    closer = getattr(client, "aclose", None) or getattr(client, "close", None)
    if closer is None:
        return
    result = closer()
    if hasattr(result, "__await__"):
        await result


async def _redis_call(client: Any, method_name: str, *args: Any, **kwargs: Any) -> Any:
    method = getattr(client, method_name, None)
    if not callable(method):
        return None
    result = method(*args, **kwargs)
    if hasattr(result, "__await__"):
        return await result
    return result


async def _publish_control_event(action: str, payload: dict[str, Any] | None = None) -> bool:
    client = _redis_client()
    if client is None:
        return False
    body = {
        "event_type": "control",
        "action": action,
        "environment": _environment(),
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload or {},
    }
    try:
        await client.xadd(f"{_stream_prefix()}.control", {"payload": json.dumps(body, ensure_ascii=False)})
        return True
    except Exception as exc:
        logger.warning("Failed to publish Telegram control event: %s", exc)
        return False
    finally:
        await _close_client(client)


async def _record_audit(
    actor: str,
    command: str,
    target_type: str | None = None,
    target_id: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    conn = await _db_connect()
    if conn is None:
        return

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
    except Exception as exc:
        logger.warning("Failed to append Telegram audit log: %s", exc)
    finally:
        await conn.close()


async def _latest_system_state() -> SystemState:
    conn = await _db_connect()
    if conn is None:
        return SystemState.NORMAL

    queries = (
        f"SELECT to_state AS state FROM {_schema()}.system_state_log WHERE to_state IS NOT NULL ORDER BY created_at DESC LIMIT 1",
        f"SELECT state AS state FROM {_schema()}.system_state_log ORDER BY created_at DESC LIMIT 1",
    )
    try:
        for query in queries:
            try:
                row = await conn.fetchrow(query)
            except Exception:
                continue
            raw = _row_value(row, "state")
            if raw:
                return SystemState(str(raw))
    except Exception:
        return SystemState.NORMAL
    finally:
        await conn.close()
    return SystemState.NORMAL


async def _load_system_state_machine() -> SystemStateMachine:
    state = await _latest_system_state()
    manager = SystemStateMachine(
        initial=state,
        environment=_environment(),
        redis_url=_redis_url(),
        stream_prefix=_stream_prefix(),
        dsn=_dsn(),
    )
    if state in {SystemState.HALTED, SystemState.EMERGENCY_STOP}:
        manager._manual_override = state  # type: ignore[attr-defined]
    return manager


def _runtime_mode_key() -> str:
    return f"{_environment()}:runtime:operating_mode"


async def _get_runtime_mode() -> OperatingMode:
    conn = await _db_connect()
    if conn is not None:
        try:
            row = await conn.fetchrow(f"SELECT value FROM {_schema()}.runtime_config WHERE key = 'operating_mode'")
            raw = _row_value(row, "value")
            if raw:
                return normalize_mode(str(raw))
        except Exception:
            pass
        finally:
            await conn.close()

    client = _redis_client()
    if client is not None:
        try:
            raw = await client.get(_runtime_mode_key())
            if raw:
                return normalize_mode(str(raw))
        except Exception:
            pass
        finally:
            await _close_client(client)

    raw = _FALLBACK_RUNTIME_MODES.get(_environment()) or os.getenv("OPERATING_MODE", "READ_ONLY")
    try:
        return normalize_mode(raw)
    except ValueError:
        return OperatingMode.READ_ONLY


async def _set_runtime_mode(mode: OperatingMode, *, actor: str) -> None:
    normalized = normalize_mode(mode)
    _FALLBACK_RUNTIME_MODES[_environment()] = normalized.value
    os.environ["OPERATING_MODE"] = normalized.value

    conn = await _db_connect()
    if conn is not None:
        try:
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {_schema()}.runtime_config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_by TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await conn.execute(
                f"""
                INSERT INTO {_schema()}.runtime_config (key, value, updated_by)
                VALUES ('operating_mode', $1, $2)
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = NOW()
                """,
                normalized.value,
                actor,
            )
        except Exception as exc:
            logger.warning("Failed to persist operating mode: %s", exc)
        finally:
            await conn.close()

    client = _redis_client()
    if client is not None:
        try:
            await client.set(_runtime_mode_key(), normalized.value)
        except Exception:
            pass
        finally:
            await _close_client(client)

    await _publish_control_event("operating_mode_set", {"mode": normalized.value, "actor": actor})


async def _strategy_exists(strategy_id: str) -> bool:
    return strategy_id in _read_strategy_file_ids()


async def _strategy_mode_map(dsn: str | None, strategy_id: str) -> tuple[str, bool]:
    default_mode = "PAPER"
    if asyncpg is None or not dsn:
        return _FALLBACK_STRATEGY_MODES.get(strategy_id, (default_mode, True))

    try:
        conn = await asyncpg.connect(dsn)
        try:
            row = await conn.fetchrow(
                f"SELECT mode, COALESCE(is_active, TRUE) AS is_active FROM {_schema()}.strategy_modes WHERE strategy_id = $1",
                strategy_id,
            )
            if row is None:
                return _FALLBACK_STRATEGY_MODES.get(strategy_id, (default_mode, True))
            return str(_row_value(row, "mode", default_mode) or default_mode), bool(_row_value(row, "is_active", True))
        finally:
            await conn.close()
    except Exception:
        return _FALLBACK_STRATEGY_MODES.get(strategy_id, (default_mode, True))


async def _set_strategy_mode(
    strategy_id: str,
    mode: str,
    is_active: bool,
    *,
    actor: str,
) -> None:
    _FALLBACK_STRATEGY_MODES[strategy_id] = (mode, is_active)
    dsn = _dsn()
    if asyncpg is not None and dsn:
        try:
            conn = await asyncpg.connect(dsn)
            try:
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {_schema()}.strategy_modes (
                        strategy_id TEXT PRIMARY KEY,
                        mode TEXT NOT NULL,
                        is_active BOOLEAN NOT NULL DEFAULT TRUE,
                        weight NUMERIC(10,6) DEFAULT 1,
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
        except Exception as exc:
            logger.warning("Failed to persist strategy mode: %s", exc)

    await _publish_control_event(
        "strategy_mode_set",
        {"strategy_id": strategy_id, "mode": mode, "is_active": is_active, "actor": actor},
    )


async def _history_ok_for_strategy(strategy_id: str) -> bool:
    conn = await _db_connect()
    if conn is None:
        return True

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
        if history is not None and _row_value(history, "first_event") is not None and _row_value(history, "last_event") is not None:
            span = _row_value(history, "last_event") - _row_value(history, "first_event")
            if span >= timedelta(days=14) and int(_row_value(history, "hit_count", 0) or 0) >= 2:
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
        if span_rows is not None and _row_value(span_rows, "first_fill") is not None and _row_value(span_rows, "last_fill") is not None:
            span = _row_value(span_rows, "last_fill") - _row_value(span_rows, "first_fill")
            return span >= timedelta(days=14) and int(_row_value(span_rows, "trade_count", 0) or 0) > 0

        return False
    except Exception:
        return False
    finally:
        await conn.close()


async def _fetch_open_position_count(account_id: str) -> int:
    conn = await _db_connect()
    if conn is None:
        return 0

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
        return int(_row_value(row, "cnt", 0) or 0)
    except Exception:
        return 0
    finally:
        await conn.close()


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
        f"제안: 지정가 {price}원 x {quantity}주",
        f"손절: -{stop_loss}% / 익절: +{tps[0]}%, +{tps[1]}% / 시간손절: {time_stop}분",
        "",
    ]

    if _approval_mode():
        lines.append("상태: 수동 승인 대기 (만료 2분)")
        lines.append(f"명령어: /approve {order_intent_id}" if order_intent_id else "명령어: /approve <ORDER_INTENT_ID>")
    else:
        lines.append("상태: 모의 자동발주")
    return "\n".join(lines)


def _redis_entries(rows: Iterable[Any]) -> Iterable[tuple[Any, dict[str, Any]]]:
    for entry in rows or []:
        if not isinstance(entry, (tuple, list)) or len(entry) != 2:
            continue
        first, second = entry
        if isinstance(second, dict):
            yield first, second
            continue
        if isinstance(second, list):
            for nested in second:
                if isinstance(nested, (tuple, list)) and len(nested) == 2 and isinstance(nested[1], dict):
                    yield nested[0], nested[1]


def _parse_signal_stream_payload(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if not isinstance(raw, str):
        logger.warning("Skipping signal stream message without string payload")
        return None
    try:
        payload = json.loads(raw)
    except Exception as exc:
        logger.warning("Skipping malformed signal stream payload: %s", exc)
        return None
    if not isinstance(payload, dict):
        logger.warning("Skipping non-object signal stream payload")
        return None
    return payload


def _candidate_payload_from_signal_message(message: dict[str, Any]) -> dict[str, Any] | None:
    event_type = str(message.get("event_type", ""))
    if event_type == "news_candidate":
        return message
    if event_type == EventType.SIGNAL.value:
        payload = message.get("payload")
        if isinstance(payload, dict) and payload.get("event_type") == "news_candidate":
            return payload
    return None


def _candidate_cursor_key() -> str:
    return f"{_environment()}.bot.candidate_cursor"


def _candidate_dedup_key() -> str:
    return f"{_environment()}.bot.candidate_dedup"


async def _store_candidate_cursor(client: Any, cursor: str) -> None:
    try:
        await _redis_call(client, "set", _candidate_cursor_key(), cursor)
    except Exception as exc:
        logger.warning("Failed to persist Telegram candidate cursor: %s", exc)


async def _load_candidate_cursor(client: Any, stream: str) -> str:
    try:
        raw = await _redis_call(client, "get", _candidate_cursor_key())
    except Exception as exc:
        logger.warning("Failed to load Telegram candidate cursor: %s", exc)
        raw = None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()

    try:
        rows = await _redis_call(client, "xrevrange", stream, count=1)
    except Exception as exc:
        logger.warning("Failed to initialize Telegram candidate cursor from stream tail: %s", exc)
        rows = None
    for message_id, _fields in _redis_entries(rows or []):
        cursor = str(message_id)
        await _store_candidate_cursor(client, cursor)
        return cursor
    return "$"


def _candidate_dedup_value(payload: dict[str, Any], message_id: str) -> str:
    for key in ("order_intent_id", "event_id"):
        value = str(payload.get(key, "")).strip()
        if value:
            return f"{key}:{value}"
    return f"redis:{message_id}"


async def _reserve_candidate_notification(client: Any, payload: dict[str, Any], message_id: str) -> bool:
    value = _candidate_dedup_value(payload, message_id)
    try:
        added = await _redis_call(client, "sadd", _candidate_dedup_key(), value)
        await _redis_call(client, "expire", _candidate_dedup_key(), 7 * 24 * 60 * 60)
    except Exception as exc:
        logger.warning("Failed to reserve Telegram candidate dedup key: %s", exc)
        return True
    if added == 0:
        return False
    return True


async def _send_candidate_notifications(payload: dict[str, Any], user_ids: list[int], token: str | None) -> None:
    text = _format_candidate(payload)
    bot = Bot(token=token)
    for user_id in user_ids:
        try:
            await bot.send_message(chat_id=user_id, text=text)
        except Exception as exc:
            logger.warning("Failed to send candidate notification to %s: %s", user_id, exc)


async def _fetch_candidate_payloads(symbol: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    client = _redis_client()
    if client is None:
        return []
    stream = f"{_stream_prefix()}.{EventType.SIGNAL.value}"

    payloads: list[dict[str, Any]] = []
    try:
        rows = await client.xrevrange(stream, count=300)
        for _message_id, fields in _redis_entries(rows):
            raw = fields.get("payload") if isinstance(fields, dict) else None
            payload = _parse_signal_stream_payload(raw)
            if payload is None:
                continue
            if payload.get("event_type") != "news_candidate":
                continue
            if symbol is not None and str(payload.get("code") or payload.get("symbol", "")) != symbol:
                continue
            payloads.append(payload)
            if len(payloads) >= limit:
                break
    except Exception:
        return []
    finally:
        await _close_client(client)

    return payloads


async def _fetch_recent_reports(symbol: str, limit: int = 5) -> list[dict[str, Any]]:
    payloads = await _fetch_candidate_payloads(symbol=symbol, limit=limit)
    reports: list[dict[str, Any]] = []
    for payload in payloads:
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
    return reports


async def _fetch_account_snapshot(account_id: str) -> dict[str, Any] | None:
    conn = await _db_connect()
    if conn is None:
        return None

    queries = (
        (
            f"""
            SELECT account_id, cash_balance, available_cash, currency, updated_at
            FROM {_schema()}.accounts
            WHERE account_id = $1
            """,
            "updated_at",
        ),
        (
            f"""
            SELECT account_id, cash_balance, available_cash, 'KRW' AS currency, snapshot_time AS updated_at
            FROM {_schema()}.cash_snapshots
            WHERE account_id = $1
            ORDER BY snapshot_time DESC
            LIMIT 1
            """,
            "updated_at",
        ),
    )
    try:
        for query, _ in queries:
            try:
                row = await conn.fetchrow(query, account_id)
            except Exception:
                continue
            if row is not None:
                return {
                    "account_id": _row_value(row, "account_id", account_id),
                    "cash_balance": _row_value(row, "cash_balance", "0"),
                    "available_cash": _row_value(row, "available_cash", "0"),
                    "currency": _row_value(row, "currency", "KRW"),
                    "updated_at": _row_value(row, "updated_at"),
                }
    finally:
        await conn.close()
    return None


async def _status_lines() -> list[str]:
    state = await _latest_system_state()
    mode = await _get_runtime_mode()
    account_id = _account_id()
    account = await _fetch_account_snapshot(account_id)
    position_count = await _fetch_open_position_count(account_id)

    lines = [
        "상태 요약:",
        f"시스템: {state.value}",
        f"운영모드: {mode.value}",
        f"계좌: {account_id}",
    ]
    if account is None:
        lines.append("잔고: 데이터 없음")
    else:
        lines.append(
            f"잔고: {_fmt_decimal(account['cash_balance'])} {account['currency']} "
            f"(가용 {_fmt_decimal(account['available_cash'])})"
        )
    lines.append(f"보유종목 수: {position_count}")
    return lines


async def _positions_lines() -> list[str]:
    conn = await _db_connect()
    if conn is None:
        return ["포지션 데이터 없음(DB 미연결)."]

    try:
        rows = await conn.fetch(
            f"""
            SELECT symbol, quantity, average_price, realized_pnl, snapshot_time
            FROM (
                SELECT DISTINCT ON (symbol)
                    symbol,
                    quantity,
                    average_price,
                    realized_pnl,
                    snapshot_time
                FROM {_schema()}.position_snapshots
                WHERE account_id = $1
                ORDER BY symbol, snapshot_time DESC
            ) s
            WHERE quantity <> 0
            ORDER BY symbol ASC
            """,
            _account_id(),
        )
    except Exception:
        rows = []
    finally:
        await conn.close()

    if not rows:
        return ["보유 포지션 없음."]

    lines = ["보유 포지션:"]
    for row in rows[:20]:
        lines.append(
            f"- {_row_value(row, 'symbol')}: qty={_fmt_decimal(_row_value(row, 'quantity'))}, "
            f"avg={_fmt_decimal(_row_value(row, 'average_price'))}, "
            f"realized_pnl={_fmt_decimal(_row_value(row, 'realized_pnl'))}"
        )
    return lines


async def _today_lines() -> list[str]:
    conn = await _db_connect()
    if conn is None:
        return ["금일 거래 데이터 없음(DB 미연결)."]

    fill_count = 0
    notional = Decimal("0")
    slippage_count = 0
    slippage_pnl = Decimal("0")
    try:
        row = await conn.fetchrow(
            f"""
            SELECT
                COUNT(*) AS fill_count,
                COALESCE(SUM(quantity * price), 0) AS notional
            FROM {_schema()}.fills
            WHERE filled_at >= date_trunc('day', NOW())
            """
        )
        fill_count = int(_row_value(row, "fill_count", 0) or 0)
        notional = _to_decimal(_row_value(row, "notional", "0"))
    except Exception:
        pass

    try:
        row = await conn.fetchrow(
            f"""
            SELECT
                COUNT(*) AS trade_count,
                COALESCE(SUM((filled_price - intended_price) * quantity), 0) AS slippage_pnl
            FROM {_schema()}.slippage_records
            WHERE filled_at >= date_trunc('day', NOW())
            """
        )
        slippage_count = int(_row_value(row, "trade_count", 0) or 0)
        slippage_pnl = _to_decimal(_row_value(row, "slippage_pnl", "0"))
    except Exception:
        pass
    finally:
        await conn.close()

    return [
        "금일 요약:",
        f"체결 건수: {fill_count}",
        f"거래대금: {_fmt_decimal(notional)}",
        f"슬리피지 기록: {slippage_count}",
        f"실현/슬리피지 PnL 추정: {_fmt_decimal(slippage_pnl)}",
    ]


async def _watch_lines() -> list[str]:
    payloads = await _fetch_candidate_payloads(limit=10)
    if not payloads:
        return ["감시 중 종목 데이터 없음."]

    lines = ["감시 중 종목:"]
    seen: set[str] = set()
    for payload in payloads:
        symbol = str(payload.get("code") or payload.get("symbol", "")).strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        reason = str(payload.get("watch_reason") or payload.get("reason") or payload.get("news_summary") or "").strip()
        score = str(payload.get("confidence", ""))
        lines.append(f"- {symbol}: score={score} {reason}".rstrip())
    return lines if len(lines) > 1 else ["감시 중 종목 데이터 없음."]


async def _regime_lines() -> list[str]:
    client = _redis_client()
    if client is None:
        return ["시장 국면: 미산출"]
    try:
        raw = await client.get(f"{_environment()}:regime:current")
        if raw:
            return [f"시장 국면: {raw}"]
        rows = await client.xrevrange(f"{_stream_prefix()}.regime", count=1)
        for _message_id, fields in _redis_entries(rows):
            payload_raw = fields.get("payload")
            if not isinstance(payload_raw, str):
                continue
            try:
                payload = json.loads(payload_raw)
            except Exception:
                continue
            regime = payload.get("regime") or payload.get("payload", {}).get("regime")
            if regime:
                return [f"시장 국면: {regime}"]
    except Exception:
        return ["시장 국면: 미산출"]
    finally:
        await _close_client(client)
    return ["시장 국면: 미산출"]


async def _logs_lines() -> list[str]:
    conn = await _db_connect()
    if conn is None:
        return ["최근 로그 데이터 없음(DB 미연결)."]

    lines = ["최근 경고/상태 로그:"]
    try:
        rows = await conn.fetch(
            f"""
            SELECT created_at, stage, reason
            FROM {_schema()}.risk_events
            WHERE passed = FALSE
            ORDER BY created_at DESC
            LIMIT 5
            """
        )
        for row in rows:
            lines.append(f"- risk {str(_row_value(row, 'stage', ''))}: {str(_row_value(row, 'reason', ''))}")
    except Exception:
        pass

    state_queries = (
        f"""
        SELECT created_at, to_state AS state, reason
        FROM {_schema()}.system_state_log
        ORDER BY created_at DESC
        LIMIT 5
        """,
        f"""
        SELECT created_at, state, reason
        FROM {_schema()}.system_state_log
        ORDER BY created_at DESC
        LIMIT 5
        """,
    )
    try:
        for query in state_queries:
            try:
                rows = await conn.fetch(query)
            except Exception:
                continue
            for row in rows:
                lines.append(f"- state {str(_row_value(row, 'state', ''))}: {str(_row_value(row, 'reason', ''))}")
            break
    finally:
        await conn.close()

    return lines if len(lines) > 1 else ["최근 경고/상태 로그 없음."]


async def _strategy_modes_all() -> dict[str, tuple[str, bool]]:
    out = dict(_FALLBACK_STRATEGY_MODES)
    conn = await _db_connect()
    if conn is None:
        return out
    try:
        rows = await conn.fetch(f"SELECT strategy_id, mode, COALESCE(is_active, TRUE) AS is_active FROM {_schema()}.strategy_modes")
        for row in rows:
            out[str(_row_value(row, "strategy_id"))] = (
                str(_row_value(row, "mode", "PAPER")),
                bool(_row_value(row, "is_active", True)),
            )
    except Exception:
        pass
    finally:
        await conn.close()
    return out


async def _strategies_lines() -> list[str]:
    ids = set(_read_strategy_file_ids())
    modes = await _strategy_modes_all()
    ids.update(modes.keys())
    if not ids:
        return ["전략 목록 없음."]

    lines = ["전략 목록:"]
    for strategy_id in sorted(ids):
        mode, active = modes.get(strategy_id, ("PAPER", True))
        status = "active" if active else "disabled"
        source = "file" if strategy_id in _read_strategy_file_ids() else "db"
        lines.append(f"- {strategy_id}: {mode}, {status}, source={source}")
    return lines


async def _journal_lines(day_text: str | None) -> list[str]:
    if not day_text:
        return ["사용법: /journal YYYY-MM-DD"]
    try:
        day = date.fromisoformat(day_text)
    except ValueError:
        return ["날짜 형식 오류: YYYY-MM-DD"]

    conn = await _db_connect()
    if conn is None:
        return [f"{day.isoformat()} 매매일지 데이터 없음(DB 미연결)."]

    try:
        rows = await conn.fetch(
            f"""
            SELECT strategy_id, symbol_code, pnl, pnl_pct, narrative, lessons, COALESCE(exit_at, created_at) AS event_time
            FROM {_schema()}.journal_entries
            WHERE DATE(COALESCE(exit_at, created_at)) = $1
            ORDER BY COALESCE(exit_at, created_at) ASC
            LIMIT 20
            """,
            day,
        )
    except Exception:
        rows = []
    finally:
        await conn.close()

    if not rows:
        return [f"{day.isoformat()} 매매일지 없음."]
    lines = [f"{day.isoformat()} 매매일지:"]
    for row in rows:
        narrative = str(_row_value(row, "narrative", "") or _row_value(row, "lessons", "") or "").strip()
        lines.append(
            f"- {str(_row_value(row, 'symbol_code', ''))} "
            f"{str(_row_value(row, 'strategy_id', ''))}: pnl={_fmt_decimal(_row_value(row, 'pnl', '0'))} "
            f"pct={_fmt_decimal(_row_value(row, 'pnl_pct', '0'))} {narrative}".rstrip()
        )
    return lines


def _is_live_mode(mode: OperatingMode) -> bool:
    return mode in {OperatingMode.LIVE_APPROVAL, OperatingMode.LIVE_AUTO}


async def cmd_daily_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    await _record_audit(actor=actor, command="/daily_report")
    await _reply(update, "일일 보고서: 구현 필요(요구 시 수동 호출).")


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    await _record_audit(actor=actor, command="/status")
    await _reply(update, "\n".join(await _status_lines()))


async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    args = list(getattr(context, "args", []) or [])
    if not args:
        mode = await _get_runtime_mode()
        await _record_audit(actor=actor, command="/mode", payload={"mode": mode.value})
        await _reply(update, f"운영모드: {mode.value}")
        return

    if len(args) < 2 or str(args[0]).lower() != "set":
        await _record_audit(actor=actor, command="/mode", payload={"args": args, "status": "invalid_usage"})
        await _reply(update, "사용법: /mode 또는 /mode set MODE [--confirm]")
        return

    if not _allowed(update):
        await _record_audit(actor=actor, command="/mode", payload={"args": args, "status": "forbidden"})
        await _reply(update, "권한이 없습니다.")
        return

    try:
        target_mode = normalize_mode(str(args[1]))
    except ValueError as exc:
        await _record_audit(actor=actor, command="/mode", payload={"args": args, "status": "invalid_mode"})
        await _reply(update, str(exc))
        return

    if _is_live_mode(target_mode) and "--confirm" not in args[2:]:
        await _record_audit(actor=actor, command="/mode", payload={"mode": target_mode.value, "status": "missing_confirm"})
        await _reply(update, "LIVE 계열 모드는 --confirm 이 필요합니다.")
        return

    await _set_runtime_mode(target_mode, actor=actor)
    await _record_audit(actor=actor, command="/mode", target_type="operating_mode", target_id=target_mode.value, payload={"status": "applied"})
    await _reply(update, f"운영모드 변경 완료: {target_mode.value}")


async def cmd_positions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    await _record_audit(actor=actor, command="/positions")
    await _reply(update, "\n".join(await _positions_lines()))


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    await _record_audit(actor=actor, command="/today")
    await _reply(update, "\n".join(await _today_lines()))


async def cmd_watch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    await _record_audit(actor=actor, command="/watch")
    await _reply(update, "\n".join(await _watch_lines()))


async def cmd_regime(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    await _record_audit(actor=actor, command="/regime")
    await _reply(update, "\n".join(await _regime_lines()))


async def cmd_logs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    await _record_audit(actor=actor, command="/logs")
    await _reply(update, "\n".join(await _logs_lines()))


async def cmd_strategies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    await _record_audit(actor=actor, command="/strategies")
    await _reply(update, "\n".join(await _strategies_lines()))


async def cmd_journal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    args = list(getattr(context, "args", []) or [])
    target_date = str(args[0]).strip() if args else None
    await _record_audit(actor=actor, command="/journal", target_type="date", target_id=target_date)
    await _reply(update, "\n".join(await _journal_lines(target_date)))


async def cmd_briefing(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    published = await _publish_control_event("daily_briefing_requested", {"actor": actor})
    await _record_audit(actor=actor, command="/briefing", payload={"published": published})
    await _reply(update, "브리핑 트리거 접수" if published else "브리핑 트리거 접수(로컬, Redis 미연결)")


async def cmd_approve(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    args = list(getattr(context, "args", []) or [])
    if not _allowed(update):
        await _record_audit(actor=actor, command="/approve", target_type="order_intent", target_id=(args[0] if args else None))
        await _reply(update, "권한이 없습니다.")
        return

    if not args:
        await _record_audit(actor=actor, command="/approve", target_type="order_intent")
        await _reply(update, "사용법: /approve {ORDER_INTENT_ID}")
        return

    order_intent_id = str(args[0]).strip()
    token = await get_pending_token(order_intent_id, environment=_environment(), redis_url=_redis_url())
    if token is None:
        await _record_audit(
            actor=actor,
            command="/approve",
            target_type="order_intent",
            target_id=order_intent_id,
            payload={"status": "not_pending"},
        )
        await _reply(update, "현재 승인 대기 토큰이 없습니다.")
        return

    ok = await approve_order_intent(order_intent_id, token, environment=_environment(), redis_url=_redis_url())
    status = await order_intent_status(order_intent_id, environment=_environment(), redis_url=_redis_url())
    await _record_audit(
        actor=actor,
        command="/approve",
        target_type="order_intent",
        target_id=order_intent_id,
        payload={"status": status if ok else status},
    )
    await _reply(update, "주문이 승인되었습니다." if ok else f"승인되지 않음: {status or 'unknown'}")


async def cmd_reject(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    args = list(getattr(context, "args", []) or [])
    if not _allowed(update):
        await _record_audit(actor=actor, command="/reject", target_type="order_intent", target_id=(args[0] if args else None))
        await _reply(update, "권한이 없습니다.")
        return

    if not args:
        await _record_audit(actor=actor, command="/reject", target_type="order_intent")
        await _reply(update, "사용법: /reject {ORDER_INTENT_ID}")
        return

    order_intent_id = str(args[0]).strip()
    ok = await reject_order_intent(order_intent_id, environment=_environment(), redis_url=_redis_url())
    await _record_audit(
        actor=actor,
        command="/reject",
        target_type="order_intent",
        target_id=order_intent_id,
        payload={"status": "RISK_REJECTED" if ok else "FAILED"},
    )
    await _reply(update, "거부 처리되었습니다." if ok else "거부 처리 실패")


async def cmd_why(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    args = list(getattr(context, "args", []) or [])
    if not args:
        await _record_audit(actor=actor, command="/why", target_type="symbol")
        await _reply(update, "사용법: /why {SYMBOL}")
        return

    symbol = str(args[0]).strip()
    reports = await _fetch_recent_reports(symbol)
    await _record_audit(actor=actor, command="/why", target_type="symbol", target_id=symbol)

    if not reports:
        await _reply(update, f"{symbol} 관련 watch reason가 없습니다.")
        return

    reason = reports[0].get("reason") or reports[0].get("news_summary") or reports[0].get("technical_summary")
    lines = [f"{symbol} 왜 보고 있나?", f"사유: {reason}", "최근 신호:"]
    for report in reports:
        lines.append(f"- {report['strategy_id']} score={report['confidence']} regime={report['regime']}")
    await _reply(update, "\n".join(lines))


async def cmd_risk(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    await _record_audit(actor=actor, command="/risk")

    manager = RiskManager()
    daily, weekly, _ = await manager.evaluate_loss_limits(_account_id(), schema=_schema())
    lines = [
        "리스크 현황:",
        f"일일 손실: {daily.loss} / 제한 {daily.limit} (사용률 {daily.used_ratio * Decimal('100')}%)",
        f"주간 손실: {weekly.loss} / 제한 {weekly.limit} (사용률 {weekly.used_ratio * Decimal('100')}%)",
        f"실사용률: {manager.risk_usage_pct.get('daily_pct', Decimal('0'))}%",
    ]
    await _reply(update, "\n".join(lines))


async def cmd_halt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    if not _allowed(update):
        await _record_audit(actor=actor, command="/halt", payload={"status": "forbidden"})
        await _reply(update, "권한이 없습니다.")
        return

    manager = await _load_system_state_machine()
    manager.cancel_open_orders_only(reason="telegram_halt", actor=actor)
    published = await _publish_control_event("cancel_open_orders", {"actor": actor, "state": manager.state.value, "scope": "open_orders"})
    await _record_audit(
        actor=actor,
        command="/halt",
        payload={"state": manager.state.value, "cancel_open_orders": True, "published": published},
    )
    await _reply(update, f"HALT 적용: 상태={manager.state.value}, 미체결 취소 트리거={'전송' if published else '로컬'}")


async def cmd_promote(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    args = list(getattr(context, "args", []) or [])
    if not _allowed(update):
        await _record_audit(actor=actor, command="/promote", payload={"args": args, "status": "forbidden"})
        await _reply(update, "권한이 없습니다.")
        return

    ok, strategy_or_error, confirmed = _parse_promote_args(args)
    if not ok:
        await _record_audit(actor=actor, command="/promote", payload={"args": args, "status": "invalid_usage"})
        await _reply(update, str(strategy_or_error))
        return
    strategy_id = str(strategy_or_error)

    if not confirmed:
        await _record_audit(actor=actor, command="/promote", target_type="strategy", target_id=strategy_id, payload={"status": "missing_confirm"})
        await _reply(update, "--confirm 이 필요합니다.")
        return

    if not await _strategy_exists(strategy_id):
        await _record_audit(actor=actor, command="/promote", target_type="strategy", target_id=strategy_id, payload={"status": "unknown_strategy"})
        await _reply(update, f"알 수 없는 전략: {strategy_id}")
        return

    if not await _history_ok_for_strategy(strategy_id):
        await _record_audit(actor=actor, command="/promote", target_type="strategy", target_id=strategy_id, payload={"status": "history_blocked"})
        await _reply(update, f"승격 차단: {strategy_id}의 검증 이력이 부족합니다.")
        return

    await _set_strategy_mode(strategy_id, "LIVE_AUTO", True, actor=actor)
    await _record_audit(actor=actor, command="/promote", target_type="strategy", target_id=strategy_id, payload={"mode": "LIVE_AUTO", "status": "applied"})
    await _reply(update, f"전략 승격 완료: {strategy_id} -> LIVE_AUTO")


async def _resume_to_mode(update: Update, target_mode: OperatingMode, command: str) -> None:
    actor = _actor(update)
    if not _allowed(update):
        await _record_audit(actor=actor, command=command, payload={"status": "forbidden"})
        await _reply(update, "권한이 없습니다.")
        return

    manager = await _load_system_state_machine()
    if manager.state in {SystemState.HALTED, SystemState.EMERGENCY_STOP}:
        allowed, reason = manager.can_resume_command
        if not allowed:
            await _record_audit(actor=actor, command=command, payload={"status": "blocked", "reason": reason})
            await _reply(update, f"재개 차단: {reason}")
            return
        manager.human_resume(reason=command.strip("/"), actor="telegram", source=actor)

    await _set_runtime_mode(target_mode, actor=actor)
    published = await _publish_control_event("resume", {"actor": actor, "mode": target_mode.value, "state": manager.state.value})
    await _record_audit(actor=actor, command=command, payload={"mode": target_mode.value, "state": manager.state.value, "published": published})
    await _reply(update, f"재개 완료: 상태={manager.state.value}, 운영모드={target_mode.value}")


async def cmd_resume_live(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _resume_to_mode(update, OperatingMode.LIVE_APPROVAL, "/resume_live")


async def cmd_resume_paper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _resume_to_mode(update, OperatingMode.PAPER, "/resume_paper")


async def cmd_disable(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    args = list(getattr(context, "args", []) or [])
    if not _allowed(update):
        await _record_audit(actor=actor, command="/disable", payload={"args": args, "status": "forbidden"})
        await _reply(update, "권한이 없습니다.")
        return

    ok, strategy_or_error = _parse_disable_args(args)
    if not ok:
        await _record_audit(actor=actor, command="/disable", payload={"args": args, "status": "invalid_usage"})
        await _reply(update, str(strategy_or_error))
        return
    strategy_id = str(strategy_or_error)
    if not await _strategy_exists(strategy_id):
        await _record_audit(actor=actor, command="/disable", target_type="strategy", target_id=strategy_id, payload={"status": "unknown_strategy"})
        await _reply(update, f"알 수 없는 전략: {strategy_id}")
        return

    current_mode, _ = await _strategy_mode_map(_dsn(), strategy_id)
    await _set_strategy_mode(strategy_id, current_mode, False, actor=actor)
    await _record_audit(actor=actor, command="/disable", target_type="strategy", target_id=strategy_id, payload={"mode": current_mode, "is_active": False})
    await _reply(update, f"전략 비활성화 완료: {strategy_id}")


async def cmd_disable_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    args = list(getattr(context, "args", []) or [])
    symbol = str(args[0]).strip().upper() if args else ""
    if not _allowed(update):
        await _record_audit(actor=actor, command="/disable_symbol", target_type="symbol", target_id=symbol, payload={"status": "forbidden"})
        await _reply(update, "권한이 없습니다.")
        return
    if not symbol:
        await _record_audit(actor=actor, command="/disable_symbol", payload={"status": "invalid_usage"})
        await _reply(update, "사용법: /disable_symbol CODE")
        return

    await set_trading_control(
        "symbol",
        symbol,
        blocked=True,
        reason="telegram_disable_symbol",
        actor=actor,
        environment=_environment(),
        dsn=_dsn(),
        redis_url=_redis_url(),
        stream_prefix=_stream_prefix(),
    )
    await _record_audit(actor=actor, command="/disable_symbol", target_type="symbol", target_id=symbol, payload={"blocked": True})
    await _reply(update, f"종목 신규 진입 차단 완료: {symbol}")


async def cmd_disable_market(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    actor = _actor(update)
    args = list(getattr(context, "args", []) or [])
    market = str(args[0]).strip().upper() if args else ""
    if not _allowed(update):
        await _record_audit(actor=actor, command="/disable_market", target_type="market", target_id=market, payload={"status": "forbidden"})
        await _reply(update, "권한이 없습니다.")
        return
    if market not in {"KR", "US"}:
        await _record_audit(actor=actor, command="/disable_market", target_type="market", target_id=market, payload={"status": "invalid_usage"})
        await _reply(update, "사용법: /disable_market KR|US")
        return

    await set_trading_control(
        "market",
        market,
        blocked=True,
        reason="telegram_disable_market",
        actor=actor,
        environment=_environment(),
        dsn=_dsn(),
        redis_url=_redis_url(),
        stream_prefix=_stream_prefix(),
    )
    await _record_audit(actor=actor, command="/disable_market", target_type="market", target_id=market, payload={"blocked": True})
    await _reply(update, f"시장 신규 진입 차단 완료: {market}")


async def _candidate_listener() -> None:
    user_ids = _parse_allowed_user_ids()
    if not user_ids:
        return

    client = _redis_client()
    if client is None:
        logger.warning("Redis unavailable; Telegram candidate listener is disabled.")
        return

    # Keep typed RedisStreamBus consumers strict. The signals stream intentionally
    # carries news_candidate/news_analysis envelopes that are not core EventType
    # values, so Telegram reads raw JSON and filters only candidate notifications.
    stream = f"{_stream_prefix()}.{EventType.SIGNAL.value}"
    cursor = await _load_candidate_cursor(client, stream)
    try:
        while True:
            try:
                rows = await client.xread({stream: cursor}, count=10, block=1000)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Failed to read Telegram candidate stream: %s", exc)
                await asyncio.sleep(1)
                continue

            for message_id, fields in _redis_entries(rows):
                cursor = str(message_id)
                raw = fields.get("payload") if isinstance(fields, dict) else None
                message = _parse_signal_stream_payload(raw)
                if message is None:
                    await _store_candidate_cursor(client, cursor)
                    continue
                payload = _candidate_payload_from_signal_message(message)
                if payload is None:
                    await _store_candidate_cursor(client, cursor)
                    continue
                if not await _reserve_candidate_notification(client, payload, cursor):
                    await _store_candidate_cursor(client, cursor)
                    continue
                await _send_candidate_notifications(payload, user_ids, os.getenv("TELEGRAM_BOT_TOKEN"))
                await _store_candidate_cursor(client, cursor)
    finally:
        await _close_client(client)


async def _runner() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    normalized_token = str(token).strip() if token is not None else ""
    if normalized_token.lower() in {"", "none", "null"}:
        logger.warning("TELEGRAM_BOT_TOKEN is not configured. Telegram bot startup is skipped and process stays alive.")
        while True:
            await asyncio.sleep(3600)
        return

    app = ApplicationBuilder().token(normalized_token).build()
    handlers = {
        "daily_report": cmd_daily_report,
        "status": cmd_status,
        "mode": cmd_mode,
        "positions": cmd_positions,
        "today": cmd_today,
        "watch": cmd_watch,
        "regime": cmd_regime,
        "logs": cmd_logs,
        "briefing": cmd_briefing,
        "journal": cmd_journal,
        "strategies": cmd_strategies,
        "approve": cmd_approve,
        "reject": cmd_reject,
        "why": cmd_why,
        "risk": cmd_risk,
        "halt": cmd_halt,
        "promote": cmd_promote,
        "resume_live": cmd_resume_live,
        "resume_paper": cmd_resume_paper,
        "disable": cmd_disable,
        "disable_symbol": cmd_disable_symbol,
        "disable_market": cmd_disable_market,
    }
    for command, handler in handlers.items():
        app.add_handler(CommandHandler(command, handler))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    await _candidate_listener()

    await app.updater.stop()
    await app.stop()
    await app.shutdown()


if __name__ == "__main__":
    asyncio.run(_runner())
