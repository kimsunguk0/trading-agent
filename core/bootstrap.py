"""Boot-time environment consistency checks."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass

from .operating_mode import OperatingMode, normalize_mode


logger = logging.getLogger(__name__)


class FatalConfigError(RuntimeError):
    """Raised when paper/live boot configuration is internally inconsistent."""


@dataclass(frozen=True)
class BootstrapResult:
    environment: str
    operating_mode: OperatingMode
    redis_prefix: str
    db_schema: str
    broker_key_env: str


_VALID_ENVIRONMENTS = {"paper", "live"}
_SCHEMA_RE = re.compile(r"^trading_(paper|live)$")


def _require_environment(value: str | None) -> str:
    if value is None or not value.strip():
        raise FatalConfigError("ENVIRONMENT is required and must be 'paper' or 'live'")
    environment = value.strip().lower()
    if environment not in _VALID_ENVIRONMENTS:
        raise FatalConfigError(f"Invalid ENVIRONMENT {value!r}; expected 'paper' or 'live'")
    return environment


def _require_operating_mode(value: str | None) -> OperatingMode:
    try:
        return normalize_mode(value)
    except Exception as exc:
        raise FatalConfigError(f"Invalid OPERATING_MODE {value!r}") from exc


def _schema_suffix(schema: str) -> str:
    match = _SCHEMA_RE.fullmatch(schema)
    if match is None:
        raise FatalConfigError("DB schema must be exactly 'trading_paper' or 'trading_live'")
    return match.group(1)


def _explicit_broker_key_env() -> str | None:
    for name in ("BROKER_KEY_ENV", "BROKER_CREDENTIAL_ENV", "VAULT_BROKER_ENV"):
        value = os.getenv(name)
        if value:
            key_env = value.strip().lower()
            if key_env not in _VALID_ENVIRONMENTS:
                raise FatalConfigError(f"{name} must be 'paper' or 'live', got {value!r}")
            return key_env
    return None


def _non_empty_env_any(names: tuple[str, ...]) -> bool:
    return any(bool(os.getenv(name, "").strip()) for name in names)


def _infer_broker_key_env(environment: str) -> str:
    explicit = _explicit_broker_key_env()
    if explicit is not None:
        return explicit

    adapter = os.getenv("BROKER_ADAPTER", "simulated").strip().lower()
    if "live" in adapter:
        return "live"
    if adapter in {"toss", "toss_invest", "toss_invest_live"}:
        return "live"
    if any(token in adapter for token in ("mock", "paper", "virtual")):
        return "paper"
    if adapter == "simulated":
        return environment

    live_keys_present = _non_empty_env_any(
        (
            "KIWOOM_LIVE_APP_KEY",
            "KIWOOM_LIVE_APP_SECRET",
            "KIWOOM_LIVE_ACCOUNT_NO",
            "KIS_LIVE_APP_KEY",
            "KIS_LIVE_APP_SECRET",
            "KIS_LIVE_ACCOUNT_NO",
            "TOSS_APP_KEY",
            "TOSS_APP_SECRET",
            "TOSS_ACCOUNT_NO",
        )
    )
    paper_keys_present = _non_empty_env_any(
        (
            "APP_KEY",
            "APP_SECRET",
            "KIWOOM_APP_KEY",
            "KIWOOM_APP_SECRET",
            "KIWOOM_ACCOUNT_NO",
            "KIS_APP_KEY",
            "KIS_APP_SECRET",
            "KIS_ACCOUNT_NO",
        )
    )
    if live_keys_present and not paper_keys_present:
        return "live"
    if paper_keys_present and not live_keys_present:
        return "paper"
    return environment


def _assert_no_cross_environment_credentials(environment: str) -> None:
    live_keys_present = _non_empty_env_any(
        (
            "KIWOOM_LIVE_APP_KEY",
            "KIWOOM_LIVE_APP_SECRET",
            "KIWOOM_LIVE_ACCOUNT_NO",
            "KIS_LIVE_APP_KEY",
            "KIS_LIVE_APP_SECRET",
            "KIS_LIVE_ACCOUNT_NO",
            "TOSS_APP_KEY",
            "TOSS_APP_SECRET",
            "TOSS_ACCOUNT_NO",
        )
    )
    paper_keys_present = _non_empty_env_any(
        (
            "APP_KEY",
            "APP_SECRET",
            "KIWOOM_APP_KEY",
            "KIWOOM_APP_SECRET",
            "KIWOOM_ACCOUNT_NO",
            "KIS_APP_KEY",
            "KIS_APP_SECRET",
            "KIS_ACCOUNT_NO",
        )
    )
    if environment == "paper" and live_keys_present:
        raise FatalConfigError("Live broker credentials are present while ENVIRONMENT=paper")
    if environment == "live" and paper_keys_present:
        raise FatalConfigError("Paper broker credentials are present while ENVIRONMENT=live")


def validate_bootstrap(
    *,
    environment: str | None = None,
    operating_mode: str | None = None,
    redis_prefix: str | None = None,
    db_schema: str | None = None,
) -> BootstrapResult:
    environment = _require_environment(environment if environment is not None else os.getenv("ENVIRONMENT"))
    mode = _require_operating_mode(operating_mode if operating_mode is not None else os.getenv("OPERATING_MODE", "READ_ONLY"))

    if environment == "paper" and mode == OperatingMode.LIVE_AUTO:
        raise FatalConfigError("LIVE_AUTO requires ENVIRONMENT=live")

    redis_prefix = redis_prefix if redis_prefix is not None else os.getenv("REDIS_STREAM_PREFIX")
    if redis_prefix is None or not redis_prefix.strip():
        raise FatalConfigError("REDIS_STREAM_PREFIX is required")
    redis_prefix = redis_prefix.strip()
    if not redis_prefix.startswith(f"{environment}."):
        raise FatalConfigError(
            f"Redis stream prefix {redis_prefix!r} is inconsistent with ENVIRONMENT={environment!r}"
        )

    db_schema = db_schema if db_schema is not None else os.getenv("DB_SCHEMA", f"trading_{environment}")
    db_schema = db_schema.strip()
    db_env = _schema_suffix(db_schema)
    broker_key_env = _infer_broker_key_env(environment)
    _assert_no_cross_environment_credentials(environment)

    if not (broker_key_env == environment == db_env):
        raise FatalConfigError(
            "Environment mismatch: "
            f"env={environment}, key={broker_key_env}, db={db_schema}"
        )

    return BootstrapResult(
        environment=environment,
        operating_mode=mode,
        redis_prefix=redis_prefix,
        db_schema=db_schema,
        broker_key_env=broker_key_env,
    )


def boot_or_raise() -> BootstrapResult:
    try:
        return validate_bootstrap()
    except FatalConfigError:
        raise
    except Exception as exc:  # pragma: no cover - defensive boundary
        logger.exception("Unexpected bootstrap validation failure")
        raise FatalConfigError(str(exc)) from exc
