from __future__ import annotations

import pytest

from core.bootstrap import FatalConfigError, validate_bootstrap
from core.operating_mode import OperatingMode


_CREDENTIAL_ENV = (
    "BROKER_KEY_ENV",
    "BROKER_CREDENTIAL_ENV",
    "VAULT_BROKER_ENV",
    "APP_KEY",
    "APP_SECRET",
    "KIWOOM_APP_KEY",
    "KIWOOM_APP_SECRET",
    "KIWOOM_ACCOUNT_NO",
    "KIS_APP_KEY",
    "KIS_APP_SECRET",
    "KIS_ACCOUNT_NO",
    "KIWOOM_LIVE_APP_KEY",
    "KIWOOM_LIVE_APP_SECRET",
    "KIWOOM_LIVE_ACCOUNT_NO",
    "KIS_LIVE_APP_KEY",
    "KIS_LIVE_APP_SECRET",
    "KIS_LIVE_ACCOUNT_NO",
)


def _clear_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _CREDENTIAL_ENV:
        monkeypatch.delenv(name, raising=False)


def test_bootstrap_validates_paper_triple_consistency(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_credentials(monkeypatch)
    monkeypatch.setenv("ENVIRONMENT", "paper")
    monkeypatch.setenv("OPERATING_MODE", "PAPER")
    monkeypatch.setenv("REDIS_STREAM_PREFIX", "paper.events")
    monkeypatch.setenv("BROKER_ADAPTER", "simulated")

    result = validate_bootstrap()

    assert result.environment == "paper"
    assert result.operating_mode == OperatingMode.PAPER
    assert result.db_schema == "trading_paper"
    assert result.broker_key_env == "paper"


def test_bootstrap_rejects_mismatched_broker_key_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_credentials(monkeypatch)
    monkeypatch.setenv("ENVIRONMENT", "paper")
    monkeypatch.setenv("OPERATING_MODE", "PAPER")
    monkeypatch.setenv("REDIS_STREAM_PREFIX", "paper.events")
    monkeypatch.setenv("BROKER_KEY_ENV", "live")

    with pytest.raises(FatalConfigError):
        validate_bootstrap()


def test_bootstrap_rejects_schema_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_credentials(monkeypatch)
    monkeypatch.setenv("ENVIRONMENT", "live")
    monkeypatch.setenv("OPERATING_MODE", "LIVE_APPROVAL")
    monkeypatch.setenv("REDIS_STREAM_PREFIX", "live.events")
    monkeypatch.setenv("BROKER_ADAPTER", "kiwoom_live")
    monkeypatch.setenv("DB_SCHEMA", "trading_paper")

    with pytest.raises(FatalConfigError):
        validate_bootstrap()
