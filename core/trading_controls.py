"""Shared trading controls for symbol/market kill switches."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

try:
    import asyncpg
except Exception:  # pragma: no cover
    asyncpg = None

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None


logger = logging.getLogger(__name__)

_FALLBACK_CONTROLS: dict[tuple[str, str, str], dict[str, Any]] = {}


def _environment_key(environment: str | None) -> str:
    return (environment or "paper").strip().lower() or "paper"


def _schema(environment: str | None) -> str:
    return f"trading_{_environment_key(environment)}"


def _normalize_control_type(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized not in {"symbol", "market"}:
        raise ValueError(f"Unsupported trading control type: {value}")
    return normalized


def _normalize_target(control_type: str, target: str) -> str:
    normalized = str(target).strip().upper()
    if control_type == "market" and normalized not in {"KR", "US"}:
        raise ValueError("market control target must be KR or US")
    if not normalized:
        raise ValueError("trading control target is required")
    return normalized


def _control_field(control_type: str, target: str) -> str:
    return f"{control_type}:{target}"


def _redis_key(environment: str | None) -> str:
    return f"{_environment_key(environment)}:trading_controls"


async def _close_client(client: Any) -> None:
    closer = getattr(client, "aclose", None) or getattr(client, "close", None)
    if closer is None:
        return
    result = closer()
    if hasattr(result, "__await__"):
        await result


def _payload(
    *,
    environment: str,
    control_type: str,
    target: str,
    blocked: bool,
    reason: str,
    actor: str,
) -> dict[str, Any]:
    return {
        "environment": environment,
        "control_type": control_type,
        "target": target,
        "blocked": bool(blocked),
        "reason": reason,
        "actor": actor,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


async def _ensure_table(conn: Any, schema: str) -> None:
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.trading_controls (
            control_type TEXT NOT NULL,
            target TEXT NOT NULL,
            blocked BOOLEAN NOT NULL DEFAULT TRUE,
            reason TEXT,
            updated_by TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (control_type, target)
        )
        """
    )


async def set_trading_control(
    control_type: str,
    target: str,
    *,
    blocked: bool = True,
    reason: str = "",
    actor: str = "system",
    environment: str = "paper",
    dsn: str | None = None,
    redis_url: str | None = None,
    stream_prefix: str | None = None,
) -> None:
    """Persist a symbol/market entry control in DB, Redis, or local fallback."""

    env = _environment_key(environment)
    normalized_type = _normalize_control_type(control_type)
    normalized_target = _normalize_target(normalized_type, target)
    payload = _payload(
        environment=env,
        control_type=normalized_type,
        target=normalized_target,
        blocked=blocked,
        reason=reason,
        actor=actor,
    )

    _FALLBACK_CONTROLS[(env, normalized_type, normalized_target)] = payload

    if asyncpg is not None and dsn:
        try:
            conn = await asyncpg.connect(dsn)
            try:
                await _ensure_table(conn, _schema(env))
                await conn.execute(
                    f"""
                    INSERT INTO {_schema(env)}.trading_controls (
                        control_type,
                        target,
                        blocked,
                        reason,
                        updated_by
                    )
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (control_type, target) DO UPDATE
                    SET blocked = EXCLUDED.blocked,
                        reason = EXCLUDED.reason,
                        updated_by = EXCLUDED.updated_by,
                        updated_at = NOW()
                    """,
                    normalized_type,
                    normalized_target,
                    bool(blocked),
                    reason,
                    actor,
                )
            finally:
                await conn.close()
        except Exception as exc:  # pragma: no cover - defensive infra fallback
            logger.warning("Failed to persist trading control to Postgres: %s", exc)

    if redis is not None and redis_url:
        client = redis.from_url(redis_url, decode_responses=True)
        try:
            await client.hset(_redis_key(env), _control_field(normalized_type, normalized_target), json.dumps(payload, ensure_ascii=False))
            stream = f"{stream_prefix or f'{env}.events'}.control"
            await client.xadd(stream, {"payload": json.dumps({"event_type": "trading_control", **payload}, ensure_ascii=False)})
        except Exception as exc:  # pragma: no cover - defensive infra fallback
            logger.warning("Failed to persist trading control to Redis: %s", exc)
        finally:
            await _close_client(client)


async def _read_db_control(
    *,
    environment: str,
    symbol: str,
    market: str,
    dsn: str,
) -> dict[str, Any] | None:
    if asyncpg is None:
        return None
    try:
        conn = await asyncpg.connect(dsn)
        try:
            row = await conn.fetchrow(
                f"""
                SELECT control_type, target, blocked, reason, updated_by, updated_at
                FROM {_schema(environment)}.trading_controls
                WHERE blocked = TRUE
                  AND (
                    (control_type = 'symbol' AND target = $1)
                    OR (control_type = 'market' AND target = $2)
                  )
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                symbol,
                market,
            )
            if row is None:
                return None
            return {
                "control_type": row["control_type"],
                "target": row["target"],
                "blocked": row["blocked"],
                "reason": row["reason"],
                "updated_by": row["updated_by"],
                "updated_at": row["updated_at"],
            }
        finally:
            await conn.close()
    except Exception:
        return None


async def _read_redis_control(
    *,
    environment: str,
    symbol: str,
    market: str,
    redis_url: str,
) -> dict[str, Any] | None:
    if redis is None:
        return None
    client = redis.from_url(redis_url, decode_responses=True)
    try:
        for control_type, target in (("symbol", symbol), ("market", market)):
            raw = await client.hget(_redis_key(environment), _control_field(control_type, target))
            if raw is None:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue
            if bool(payload.get("blocked")):
                return payload
    except Exception:
        return None
    finally:
        await _close_client(client)
    return None


def _read_fallback_control(environment: str, symbol: str, market: str) -> dict[str, Any] | None:
    for control_type, target in (("symbol", symbol), ("market", market)):
        payload = _FALLBACK_CONTROLS.get((environment, control_type, target))
        if payload is not None and bool(payload.get("blocked")):
            return payload
    return None


async def is_entry_allowed(
    symbol: str,
    market: str,
    *,
    environment: str = "paper",
    dsn: str | None = None,
    redis_url: str | None = None,
    stream_prefix: str | None = None,
) -> tuple[bool, str]:
    """Return whether a new market entry is allowed for the symbol/market."""

    _ = stream_prefix
    env = _environment_key(environment)
    normalized_symbol = _normalize_target("symbol", symbol)
    normalized_market = _normalize_target("market", market)

    payload: dict[str, Any] | None = None
    if dsn:
        payload = await _read_db_control(environment=env, symbol=normalized_symbol, market=normalized_market, dsn=dsn)
    if payload is None and redis_url:
        payload = await _read_redis_control(environment=env, symbol=normalized_symbol, market=normalized_market, redis_url=redis_url)
    if payload is None:
        payload = _read_fallback_control(env, normalized_symbol, normalized_market)

    if payload is None:
        return True, "allowed"

    reason = str(payload.get("reason") or "disabled")
    control_type = str(payload.get("control_type") or "control")
    target = str(payload.get("target") or "")
    return False, f"{control_type}:{target} blocked: {reason}"


__all__ = ["set_trading_control", "is_entry_allowed", "_FALLBACK_CONTROLS"]
