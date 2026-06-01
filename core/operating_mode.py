"""Operating mode helpers and live-approval token lifecycle."""

from __future__ import annotations

import json
import secrets
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None


class ApprovalStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    RISK_REJECTED = "RISK_REJECTED"
    EXPIRED = "EXPIRED"


class OperatingMode(str, Enum):
    READ_ONLY = "READ_ONLY"
    PAPER = "PAPER"
    LIVE_APPROVAL = "LIVE_APPROVAL"
    LIVE_AUTO = "LIVE_AUTO"


# in-memory fallback for environments where redis package or server is unavailable
_FALLBACK_APPROVAL: dict[str, dict[str, Any]] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _environment_key(environment: str) -> str:
    return (environment or "paper").lower()


def normalize_mode(value: str | OperatingMode | None) -> OperatingMode:
    if value is None:
        return OperatingMode.READ_ONLY
    if isinstance(value, OperatingMode):
        return value
    try:
        return OperatingMode(value.upper())
    except ValueError as exc:
        raise ValueError(f"Unknown operating mode: {value}") from exc


def can_place_order(mode: OperatingMode) -> bool:
    return mode in {OperatingMode.PAPER, OperatingMode.LIVE_APPROVAL, OperatingMode.LIVE_AUTO}


def _approval_token_key(environment: str, token: str) -> str:
    return f"{_environment_key(environment)}:approval:{token}"


def _approval_status_key(environment: str, order_intent_id: str) -> str:
    return f"{_environment_key(environment)}:approval_status:{order_intent_id}"


def _approval_payload(
    order_intent_id: str,
    token: str,
    status: ApprovalStatus,
    *,
    expires_at: datetime,
    updated_at: datetime,
) -> str:
    return json.dumps(
        {
            "order_intent_id": order_intent_id,
            "token": token,
            "status": status.value,
            "expires_at": expires_at.isoformat(),
            "updated_at": updated_at.isoformat(),
        },
        ensure_ascii=False,
    )


def _parse_payload(raw: str) -> tuple[str, str, ApprovalStatus, datetime, datetime] | None:
    try:
        payload = json.loads(raw)
        order_intent_id = str(payload.get("order_intent_id") or "")
        token = str(payload.get("token") or "")
        status = payload.get("status")
        if not order_intent_id or not token or status not in {item.value for item in ApprovalStatus}:
            return None

        expires_raw = payload.get("expires_at")
        updated_raw = payload.get("updated_at")
        if not isinstance(expires_raw, str) or not isinstance(updated_raw, str):
            return None

        expires_at = datetime.fromisoformat(expires_raw)
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        updated_at = datetime.fromisoformat(updated_raw)
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)

        return order_intent_id, token, ApprovalStatus(status), expires_at, updated_at
    except Exception:
        return None


def _redis_client(redis_url: str | None):
    if redis is None or not redis_url:
        return None
    return redis.from_url(redis_url, decode_responses=True)


async def _close_client(client: Any) -> None:
    closer = getattr(client, "aclose", None)
    if closer is None:
        closer = getattr(client, "close", None)
    if closer is None:
        return
    result = closer()
    if hasattr(result, "__await__"):
        await result


async def publish_live_approval_token(
    order_intent_id: str,
    *,
    environment: str = "paper",
    redis_url: str | None = None,
    ttl_seconds: int = 120,
) -> str:
    env = _environment_key(environment)
    token = secrets.token_urlsafe(16)
    now = _now()
    expires_at = now + timedelta(seconds=ttl_seconds)

    client = _redis_client(redis_url)
    if client is None:
        _FALLBACK_APPROVAL[_approval_token_key(env, token)] = {
            "order_intent_id": order_intent_id,
            "token": token,
            "expires_at": expires_at.isoformat(),
            "updated_at": now.isoformat(),
            "status": ApprovalStatus.PENDING.value,
        }
        _FALLBACK_APPROVAL[_approval_status_key(env, order_intent_id)] = {
            "order_intent_id": order_intent_id,
            "token": token,
            "status": ApprovalStatus.PENDING.value,
            "expires_at": expires_at.isoformat(),
            "updated_at": now.isoformat(),
        }
        return token

    await client.setex(_approval_token_key(env, token), ttl_seconds, order_intent_id)
    await client.setex(
        _approval_status_key(env, order_intent_id),
        ttl_seconds,
        _approval_payload(order_intent_id, token, ApprovalStatus.PENDING, expires_at=expires_at, updated_at=now),
    )
    await _close_client(client)
    return token


async def _get_status_raw(
    *,
    environment: str,
    order_intent_id: str,
    redis_url: str | None = None,
) -> tuple[str, str, ApprovalStatus, datetime, datetime] | None:
    env = _environment_key(environment)
    raw_key = _approval_status_key(env, order_intent_id)

    client = _redis_client(redis_url)
    if client is None:
        payload = _FALLBACK_APPROVAL.get(raw_key)
        if payload is None:
            return None
        return _parse_payload(json.dumps(payload, ensure_ascii=False))

    raw = await client.get(raw_key)
    await _close_client(client)
    if raw is None:
        return None
    return _parse_payload(raw)


async def _lookup_order_intent(
    *,
    environment: str,
    token: str,
    redis_url: str | None = None,
) -> str | None:
    env = _environment_key(environment)
    key = _approval_token_key(env, token)

    client = _redis_client(redis_url)
    if client is None:
        payload = _FALLBACK_APPROVAL.get(key)
        if payload is None:
            return None
        return str(payload.get("order_intent_id") or "")

    value = await client.get(key)
    await _close_client(client)
    return value


async def _set_status(
    *,
    environment: str,
    order_intent_id: str,
    token: str,
    status: ApprovalStatus,
    redis_url: str | None = None,
    updated_at: datetime | None = None,
) -> bool:
    env = _environment_key(environment)
    now = updated_at or _now()
    client = _redis_client(redis_url)

    if client is None:
        status_key = _approval_status_key(env, order_intent_id)
        previous = _FALLBACK_APPROVAL.get(status_key, {})
        if not previous:
            return False

        expires_at_raw = previous.get("expires_at")
        if isinstance(expires_at_raw, str):
            expires_at = datetime.fromisoformat(expires_at_raw)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
        else:
            expires_at = now

        payload = {
            "order_intent_id": order_intent_id,
            "token": token,
            "status": status.value,
            "expires_at": expires_at.isoformat(),
            "updated_at": now.isoformat(),
        }
        _FALLBACK_APPROVAL[status_key] = payload
        return True

    status_key = _approval_status_key(env, order_intent_id)
    ttl = await client.pttl(_approval_token_key(env, token))

    if ttl > 0:
        expires_at = _now() + timedelta(milliseconds=ttl)
    elif ttl == -2:
        expires_at = now
    else:
        payload = await client.get(status_key)
        parsed = _parse_payload(payload) if payload else None
        if parsed is None:
            expires_at = now
        else:
            _status_order_intent_id, _status_token, _status, prev_expires_at, _updated = parsed
            expires_at = prev_expires_at if prev_expires_at else now

    await client.setex(
        status_key,
        ttl if ttl > 0 else 120,
        _approval_payload(
            order_intent_id,
            token,
            status,
            expires_at=expires_at,
            updated_at=now,
        ),
    )
    if status in {ApprovalStatus.APPROVED, ApprovalStatus.RISK_REJECTED}:  # no reuse
        await client.delete(_approval_token_key(env, token))
    await _close_client(client)
    return True


async def approve_order_intent(
    order_intent_id: str,
    token: str,
    *,
    environment: str = "paper",
    redis_url: str | None = None,
) -> bool:
    mapped = await _lookup_order_intent(environment=environment, token=token, redis_url=redis_url)
    if mapped != order_intent_id:
        return False

    parsed = await _get_status_raw(environment=environment, order_intent_id=order_intent_id, redis_url=redis_url)
    if parsed is None:
        return False

    _status_order_intent_id, _status_token, status, expires_at, _updated = parsed
    if status != ApprovalStatus.PENDING:
        return False
    if _status_token != token:
        return False
    if expires_at < _now():
        await _set_status(
            environment=environment,
            order_intent_id=order_intent_id,
            token=token,
            status=ApprovalStatus.EXPIRED,
            redis_url=redis_url,
        )
        return False

    return await _set_status(
        environment=environment,
        order_intent_id=order_intent_id,
        token=token,
        status=ApprovalStatus.APPROVED,
        redis_url=redis_url,
    )


async def reject_order_intent(
    order_intent_id: str,
    *,
    token: str | None = None,
    environment: str = "paper",
    redis_url: str | None = None,
) -> bool:
    if token is not None:
        mapped = await _lookup_order_intent(environment=environment, token=token, redis_url=redis_url)
        if mapped != order_intent_id:
            return False
    else:
        payload = await _get_status_raw(environment=environment, order_intent_id=order_intent_id, redis_url=redis_url)
        if payload is None:
            return False
        _, token, status, _expires, _updated = payload
        if status == ApprovalStatus.RISK_REJECTED:
            return False

    return await _set_status(
        environment=environment,
        order_intent_id=order_intent_id,
        token=token or "",
        status=ApprovalStatus.RISK_REJECTED,
        redis_url=redis_url,
    )


async def order_intent_status(
    order_intent_id: str,
    *,
    environment: str = "paper",
    redis_url: str | None = None,
) -> str:
    payload = await _get_status_raw(environment=environment, order_intent_id=order_intent_id, redis_url=redis_url)
    if payload is None:
        return ""
    return payload[2].value


async def get_pending_token(
    order_intent_id: str,
    *,
    environment: str = "paper",
    redis_url: str | None = None,
) -> str | None:
    payload = await _get_status_raw(environment=environment, order_intent_id=order_intent_id, redis_url=redis_url)
    if payload is None:
        return None
    _order_intent_id, token, status, _expires, _updated = payload
    if status != ApprovalStatus.PENDING:
        return None
    if _expires < _now():
        await _set_status(
            environment=environment,
            order_intent_id=order_intent_id,
            token=token,
            status=ApprovalStatus.EXPIRED,
            redis_url=redis_url,
        )
        return None
    return token


async def handle_order_intent_published(
    event_or_order_intent_id: Any,
    mode: str | OperatingMode,
    *,
    environment: str = "paper",
    redis_url: str | None = None,
) -> str | None:
    if normalize_mode(mode) != OperatingMode.LIVE_APPROVAL:
        return None

    order_intent_id = event_or_order_intent_id
    if hasattr(event_or_order_intent_id, "request"):
        request = getattr(event_or_order_intent_id, "request", None)
        order_intent_id = request.order_intent_id if request is not None else None

    if not isinstance(order_intent_id, str) or not order_intent_id:
        return None

    return await publish_live_approval_token(order_intent_id=order_intent_id, environment=environment, redis_url=redis_url)


async def expire_approvals(
    *,
    environment: str = "paper",
    redis_url: str | None = None,
) -> list[str]:
    env = _environment_key(environment)
    now = _now()
    expired: list[str] = []

    client = _redis_client(redis_url)
    if client is None:
        prefix = f"{env}:approval_status:"
        for key, payload in list(_FALLBACK_APPROVAL.items()):
            if not key.startswith(prefix):
                continue
            parsed = _parse_payload(json.dumps(payload, ensure_ascii=False))
            if parsed is None:
                continue
            order_intent_id, token, status, expires_at, _updated = parsed
            if status != ApprovalStatus.PENDING:
                continue
            if now < expires_at:
                continue

            _FALLBACK_APPROVAL[_approval_status_key(env, order_intent_id)] = {
                "order_intent_id": order_intent_id,
                "token": token,
                "status": ApprovalStatus.EXPIRED.value,
                "expires_at": now.isoformat(),
                "updated_at": now.isoformat(),
            }
            _FALLBACK_APPROVAL.pop(_approval_token_key(env, token), None)
            expired.append(order_intent_id)
        return expired

    keys = await client.keys(f"{env}:approval_status:*")
    for key in keys:
        raw = await client.get(key)
        if raw is None:
            continue
        parsed = _parse_payload(raw)
        if parsed is None:
            continue
        order_intent_id, token, status, expires_at, _updated = parsed
        if status != ApprovalStatus.PENDING:
            continue
        if now < expires_at:
            continue

        await _set_status(
            environment=environment,
            order_intent_id=order_intent_id,
            token=token,
            status=ApprovalStatus.EXPIRED,
            redis_url=redis_url,
            updated_at=now,
        )
        await client.delete(_approval_token_key(env, token))
        expired.append(order_intent_id)

    await _close_client(client)
    return expired
