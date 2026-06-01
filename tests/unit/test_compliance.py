"""Unit tests for ComplianceChecker."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.risk.compliance import ComplianceChecker, ComplianceResult, _ComplianceRules


def _rules(
    max_orders_per_minute: int = 30,
    max_cancel_rate_pct: Decimal = Decimal("30"),
    blacklist_symbols: frozenset[str] | None = None,
    api_rate_limit_per_second: int = 5,
    algorithmic_threshold: int = 1000,
    algorithmic_registered: bool = False,
) -> _ComplianceRules:
    return _ComplianceRules(
        version="test",
        effective_from=None,
        max_orders_per_minute=max_orders_per_minute,
        max_cancel_rate_pct=max_cancel_rate_pct,
        blacklist_symbols=blacklist_symbols or frozenset(),
        api_rate_limit_per_second=api_rate_limit_per_second,
        algorithmic_threshold=algorithmic_threshold,
        algorithmic_registered=algorithmic_registered,
        forbidden_patterns=frozenset(),
    )


def _checker(rules: _ComplianceRules) -> ComplianceChecker:
    return ComplianceChecker(rules_loader=lambda: rules, environment="paper", dsn=None)


def _order(symbol: str = "005930") -> object:
    o = MagicMock()
    o.symbol = symbol
    o.__dict__ = {"symbol": symbol}
    return o


# ─────────────────────────────────────────────────────────────────────────────
# 1. orders/minute > 30 → BLOCK
# ─────────────────────────────────────────────────────────────────────────────

def test_orders_per_minute_over_limit_is_blocked():
    checker = _checker(_rules(max_orders_per_minute=30))

    # Patch DB count to return 31 orders in last minute
    async def mock_count_recent(*args, **kwargs):
        return 31

    checker._count_recent_orders = mock_count_recent

    result = asyncio.get_event_loop().run_until_complete(checker.check_order(_order()))

    assert result.allowed is False
    assert "orders_per_minute" in result.reason


# ─────────────────────────────────────────────────────────────────────────────
# 2. cancel_rate >= 30% → BLOCK
# ─────────────────────────────────────────────────────────────────────────────

def test_cancel_rate_over_threshold_is_blocked():
    checker = _checker(_rules(max_orders_per_minute=30, max_cancel_rate_pct=Decimal("30")))

    call_count = [0]

    async def mock_count_recent(*args, **kwargs):
        call_count[0] += 1
        return 10  # 10 total orders

    async def mock_count_canceled(*args, **kwargs):
        return 3  # 3 canceled → 30% → exactly at threshold → BLOCK

    checker._count_recent_orders = mock_count_recent
    checker._count_canceled_orders = mock_count_canceled

    result = asyncio.get_event_loop().run_until_complete(checker.check_order(_order()))

    assert result.allowed is False
    assert "cancel_rate" in result.reason


def test_cancel_rate_below_threshold_is_allowed():
    checker = _checker(_rules(max_orders_per_minute=30, max_cancel_rate_pct=Decimal("30")))

    async def mock_count_recent(*args, **kwargs):
        return 10

    async def mock_count_canceled(*args, **kwargs):
        return 2  # 20% → below threshold

    async def mock_api_rate(*args, **kwargs):
        return Decimal("0")

    async def mock_daily(*args, **kwargs):
        return 5

    checker._count_recent_orders = mock_count_recent
    checker._count_canceled_orders = mock_count_canceled
    checker._load_api_rate = mock_api_rate
    checker._count_daily_orders = mock_daily

    result = asyncio.get_event_loop().run_until_complete(checker.check_order(_order()))

    assert result.allowed is True


# ─────────────────────────────────────────────────────────────────────────────
# 3. Blacklisted symbol → BLOCK
# ─────────────────────────────────────────────────────────────────────────────

def test_blacklist_symbol_is_blocked():
    rules = _rules(blacklist_symbols=frozenset(["005930", "000660"]))
    checker = _checker(rules)

    result = asyncio.get_event_loop().run_until_complete(checker.check_order(_order(symbol="005930")))

    assert result.allowed is False
    assert "blacklisted" in result.reason


def test_non_blacklisted_symbol_passes_blacklist_check():
    rules = _rules(blacklist_symbols=frozenset(["000660"]))
    checker = _checker(rules)

    async def mock_count_recent(*args, **kwargs):
        return 5

    async def mock_count_canceled(*args, **kwargs):
        return 0

    async def mock_api_rate(*args, **kwargs):
        return Decimal("0")

    async def mock_daily(*args, **kwargs):
        return 5

    checker._count_recent_orders = mock_count_recent
    checker._count_canceled_orders = mock_count_canceled
    checker._load_api_rate = mock_api_rate
    checker._count_daily_orders = mock_daily

    result = asyncio.get_event_loop().run_until_complete(checker.check_order(_order(symbol="005930")))

    assert result.allowed is True


# ─────────────────────────────────────────────────────────────────────────────
# 4. Algorithmic threshold warning (not a block)
# ─────────────────────────────────────────────────────────────────────────────

def test_algo_threshold_issues_warning_not_block():
    rules = _rules(algorithmic_threshold=1000, algorithmic_registered=False)
    checker = _checker(rules)

    async def mock_count_recent(*args, **kwargs):
        return 5

    async def mock_count_canceled(*args, **kwargs):
        return 0

    async def mock_api_rate(*args, **kwargs):
        return Decimal("0")

    async def mock_daily(*args, **kwargs):
        return 1001  # exceeds 1000

    checker._count_recent_orders = mock_count_recent
    checker._count_canceled_orders = mock_count_canceled
    checker._load_api_rate = mock_api_rate
    checker._count_daily_orders = mock_daily

    result = asyncio.get_event_loop().run_until_complete(checker.check_order(_order()))

    # WARNING only – order is still allowed
    assert result.allowed is True
    assert "WARN" in str(result.reason)
