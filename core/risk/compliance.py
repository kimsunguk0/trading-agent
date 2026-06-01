"""Deterministic compliance checks for exchange/market constraints."""

from __future__ import annotations

from collections import namedtuple
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import yaml

from core.models.order import OrderRequest, RiskCheckResult

try:
    import asyncpg
except Exception:  # pragma: no cover
    asyncpg = None


ComplianceResult = namedtuple("ComplianceResult", "allowed reason")


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    return Decimal(str(value))


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class _ComplianceRules:
    version: str
    effective_from: str | None
    max_orders_per_minute: int
    max_cancel_rate_pct: Decimal
    blacklist_symbols: frozenset[str]
    api_rate_limit_per_second: int
    algorithmic_threshold: int
    algorithmic_registered: bool
    forbidden_patterns: frozenset[str]


def load_rules(path: str | None = None) -> _ComplianceRules:
    target = Path(path or __import__("os").getenv("COMPLIANCE_CONFIG_PATH", "configs/compliance/kr_rules_2026.yaml"))
    raw: dict[str, Any] = {}
    if target.exists():
        loaded = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
        if isinstance(loaded, dict):
            raw = loaded

    section = raw.get("kr") if isinstance(raw.get("kr"), dict) else {}
    if not isinstance(section, dict):
        section = {}

    forbidden_patterns = section.get("forbidden_patterns", [])
    algorithmic = section.get("algorithmic_trader", {})
    if not isinstance(algorithmic, dict):
        algorithmic = {}

    return _ComplianceRules(
        version=str(raw.get("version", "manual")),
        effective_from=str(raw.get("effective_from")) if raw.get("effective_from") else None,
        max_orders_per_minute=max(1, int(section.get("max_orders_per_minute", 30))),
        max_cancel_rate_pct=_to_decimal(section.get("max_cancel_rate_pct", Decimal("30"))),
        blacklist_symbols=frozenset(str(item).strip().lower() for item in (section.get("blacklist_symbols", []) or []) if str(item).strip()),
        api_rate_limit_per_second=max(1, int(section.get("api_rate_limit_per_second", section.get("api_rate_limit", 5) if isinstance(section, dict) else 5))),
        algorithmic_threshold=max(1, int(algorithmic.get("threshold_orders_per_day", 1000))),
        algorithmic_registered=bool(algorithmic.get("registered", False)),
        forbidden_patterns=frozenset(str(item).strip().lower() for item in (forbidden_patterns or []) if str(item).strip()),
    )


class ComplianceChecker:
    def __init__(
        self,
        *,
        rules_loader: Callable[[], _ComplianceRules] = load_rules,
        environment: str = "paper",
        dsn: str | None = None,
        schema: str | None = None,
    ) -> None:
        self.rules = rules_loader()
        self.environment = environment
        self.dsn = dsn
        self.schema = schema or f"trading_{environment}"

    def _schema(self) -> str:
        return self.schema

    def _normalize_symbol(self, symbol: Any) -> str:
        return str(symbol or "").strip().lower()

    async def _count_recent_orders(self, start_at: datetime, *, symbol: str | None = None) -> int:
        if asyncpg is None or not self.dsn:
            return 0

        symbol_value = self._normalize_symbol(symbol)
        conn = await asyncpg.connect(self.dsn)
        try:
            try:
                if symbol_value:
                    query = f"SELECT COUNT(*) FROM {self._schema()}.order_intents WHERE created_at >= $1 AND symbol = $2"
                    count = await conn.fetchval(query, start_at, symbol_value)
                else:
                    query = f"SELECT COUNT(*) FROM {self._schema()}.order_intents WHERE created_at >= $1"
                    count = await conn.fetchval(query, start_at)
                return int(count or 0)
            except Exception:
                return 0
        finally:
            await conn.close()

    async def _count_canceled_orders(self, start_at: datetime) -> int:
        if asyncpg is None or not self.dsn:
            return 0

        conn = await asyncpg.connect(self.dsn)
        try:
            for table in ("order_history", "orders"):
                try:
                    query = f"SELECT COUNT(*) AS cnt FROM {self._schema()}.{table} WHERE created_at >= $1 AND LOWER(status) IN ('cancel', 'canceled', 'cancelled')"
                    count = await conn.fetchval(query, start_at)
                    return int(count or 0)
                except Exception:
                    continue
            return 0
        finally:
            await conn.close()

    async def _count_api_calls(self, start_at: datetime) -> int:
        if asyncpg is None or not self.dsn:
            return 0

        conn = await asyncpg.connect(self.dsn)
        try:
            try:
                query = f"SELECT COUNT(*) AS cnt FROM {self._schema()}.order_history WHERE created_at >= $1 AND event_type = 'api_call'"
                value = await conn.fetchval(query, start_at)
                return int(value or 0)
            except Exception:
                return 0
        finally:
            await conn.close()

    async def _count_daily_orders(self, start_at: datetime, *, symbol: str | None = None) -> int:
        if asyncpg is None or not self.dsn:
            return 0

        conn = await asyncpg.connect(self.dsn)
        try:
            symbol_value = self._normalize_symbol(symbol)
            try:
                if symbol_value:
                    query = f"SELECT COUNT(*) FROM {self._schema()}.order_intents WHERE created_at >= $1 AND symbol = $2"
                    value = await conn.fetchval(query, start_at, symbol_value)
                else:
                    query = f"SELECT COUNT(*) FROM {self._schema()}.order_intents WHERE created_at >= $1"
                    value = await conn.fetchval(query, start_at)
                return int(value or 0)
            except Exception:
                return 0
        finally:
            await conn.close()

    async def _load_api_rate(self, start_at: datetime) -> Decimal:
        count = await self._count_api_calls(start_at)
        window_seconds = max(Decimal("1"), _to_decimal(( _now() - start_at).total_seconds()))
        return _to_decimal(count) / window_seconds

    def _is_blocked(self, allowed: bool, reason: str) -> ComplianceResult:
        return ComplianceResult(False, reason)

    async def check_order(self, order: object) -> ComplianceResult:
        payload = order if isinstance(order, dict) else order.__dict__
        symbol = self._normalize_symbol(payload.get("symbol") if isinstance(payload, dict) else getattr(order, "symbol", ""))

        if symbol in self.rules.blacklist_symbols:
            return self._is_blocked(False, "symbol_is_blacklisted")

        if any((pattern and pattern in symbol) for pattern in self.rules.forbidden_patterns):
            return self._is_blocked(False, "symbol_contains_forbidden_pattern")

        window_start = _now() - timedelta(seconds=60)
        order_count = _to_decimal(await self._count_recent_orders(window_start, symbol=symbol))
        if order_count > _to_decimal(self.rules.max_orders_per_minute):
            return self._is_blocked(False, "orders_per_minute_exceeded")

        cancel_count = _to_decimal(await self._count_canceled_orders(window_start))
        cancel_rate = Decimal("0")
        if order_count > Decimal("0"):
            cancel_rate = (cancel_count / order_count) * Decimal("100")

        if cancel_rate >= self.rules.max_cancel_rate_pct:
            return self._is_blocked(False, "cancel_rate_exceeded")

        api_window_start = _now() - timedelta(seconds=1)
        api_rate = await self._load_api_rate(api_window_start)
        if api_rate > _to_decimal(self.rules.api_rate_limit_per_second):
            return self._is_blocked(False, "api_rate_limit_exceeded")

        daily_start = _now().replace(hour=0, minute=0, second=0, microsecond=0)
        daily_order_count = _to_decimal(await self._count_daily_orders(daily_start, symbol=symbol))
        if daily_order_count > _to_decimal(self.rules.algorithmic_threshold) and not self.rules.algorithmic_registered:
            return ComplianceResult(True, f"WARN:daily_order_count={daily_order_count}")

        return ComplianceResult(True, "")

    async def check(self, request: OrderRequest) -> RiskCheckResult:
        result = await self.check_order(request)
        if not result.allowed and not str(result.reason).startswith("WARN"):
            return RiskCheckResult(
                passed=False,
                stage="compliance",
                reason=result.reason,
                details={"config_version": self.rules.version},
            )

        return RiskCheckResult(
            passed=True,
            stage="compliance",
            reason=result.reason,
            details={"config_version": self.rules.version, "status": "warn" if str(result.reason).startswith("WARN") else "ok"},
        )


class ComplianceAgent(ComplianceChecker):
    def __init__(
        self,
        dsn: str | None = None,
        config_loader: Callable[[], _ComplianceRules] = load_rules,
        environment: str = "paper",
        schema: str | None = None,
    ) -> None:
        if dsn is None:
            import os

            dsn = os.getenv("DATABASE_URL")
        super().__init__(rules_loader=config_loader, environment=environment, dsn=dsn, schema=schema)

    async def check(self, request: Any) -> RiskCheckResult:  # pragma: no cover - thin adapter
        return await super().check(request)
