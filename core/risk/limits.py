"""Risk checks for sizing, loss limits, and approval requirements."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from core.models.order import OrderRequest, RiskCheckResult
from core.models.portfolio import Account


try:
    import asyncpg
except Exception:  # pragma: no cover
    asyncpg = None


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


@dataclass
class _LossWindow:
    loss: Decimal = Decimal("0")
    nav: Decimal = Decimal("0")
    limit: Decimal = Decimal("0")
    used_ratio: Decimal = Decimal("0")
    period_start: datetime = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass
class RiskProfile:
    max_order_notional: Decimal = Decimal("1000000")
    max_cash_ratio: Decimal = Decimal("0.95")
    max_position_pct_nav: Decimal = Decimal("0")
    max_symbol_exposure_pct_nav: Decimal = Decimal("0")
    max_daily_loss_pct_nav: Decimal = Decimal("0")
    max_weekly_loss_pct_nav: Decimal = Decimal("0")
    max_monthly_loss_pct_nav: Decimal = Decimal("0")
    max_orders_per_day: int = 0
    max_orders_per_symbol_per_day: int = 0
    max_concurrent_positions: int = 0
    cash_reserve_pct: Decimal = Decimal("0")
    allow_short: bool = False
    allow_margin: bool = False
    allow_market_order: bool = True
    require_manual_approval_for_live: bool = False


def _load_profile(path: str | None = None) -> RiskProfile:
    path = path or os.getenv("RISK_CONFIG_PATH", "")
    if path:
        config_path = Path(path)
    else:
        environment = os.getenv("ENVIRONMENT", "paper").lower()
        mode = os.getenv("OPERATING_MODE", "READ_ONLY").upper()
        if mode == "LIVE_APPROVAL":
            config_path = Path("configs/risk/live_approval.yaml")
        else:
            config_path = Path(f"configs/risk/{environment}.yaml")

    if not config_path.exists():
        return RiskProfile()

    raw: dict[str, Any] = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        return RiskProfile()

    if isinstance(raw.get("global_risk"), dict):
        raw = raw["global_risk"]

    if not isinstance(raw, dict):
        return RiskProfile()

    return RiskProfile(
        max_order_notional=_to_decimal(raw.get("max_order_notional", RiskProfile.max_order_notional)),
        max_cash_ratio=_to_decimal(raw.get("max_cash_ratio", RiskProfile.max_cash_ratio)),
        max_position_pct_nav=_to_decimal(raw.get("max_position_pct_nav", RiskProfile.max_position_pct_nav)),
        max_symbol_exposure_pct_nav=_to_decimal(
            raw.get("max_symbol_exposure_pct_nav", RiskProfile.max_symbol_exposure_pct_nav)
        ),
        max_daily_loss_pct_nav=_to_decimal(raw.get("max_daily_loss_pct_nav", RiskProfile.max_daily_loss_pct_nav)),
        max_weekly_loss_pct_nav=_to_decimal(raw.get("max_weekly_loss_pct_nav", RiskProfile.max_weekly_loss_pct_nav)),
        max_monthly_loss_pct_nav=_to_decimal(raw.get("max_monthly_loss_pct_nav", RiskProfile.max_monthly_loss_pct_nav)),
        max_orders_per_day=int(raw.get("max_orders_per_day", RiskProfile.max_orders_per_day) or 0),
        max_orders_per_symbol_per_day=int(
            raw.get("max_orders_per_symbol_per_day", RiskProfile.max_orders_per_symbol_per_day) or 0
        ),
        max_concurrent_positions=int(raw.get("max_concurrent_positions", RiskProfile.max_concurrent_positions) or 0),
        cash_reserve_pct=_to_decimal(raw.get("cash_reserve_pct", RiskProfile.cash_reserve_pct)),
        allow_short=bool(raw.get("allow_short", RiskProfile.allow_short)),
        allow_margin=bool(raw.get("allow_margin", RiskProfile.allow_margin)),
        allow_market_order=bool(raw.get("allow_market_order", RiskProfile.allow_market_order)),
        require_manual_approval_for_live=bool(
            raw.get("require_manual_approval_for_live", RiskProfile.require_manual_approval_for_live)
        ),
    )


class RiskManager:
    def __init__(
        self,
        max_order_notional: Decimal = Decimal("1000000"),
        max_cash_ratio: Decimal = Decimal("0.95"),
        risk_profile: RiskProfile | None = None,
    ) -> None:
        self.max_order_notional = max_order_notional
        self.max_cash_ratio = max_cash_ratio
        self.risk_profile = risk_profile or _load_profile()
        self._latest_window: dict[str, Decimal] = {}

    @property
    def risk_usage_pct(self) -> dict[str, Decimal]:
        return dict(self._latest_window)

    async def _fetch_cumulative_realized(self, schema: str, account_id: str, period_start: datetime) -> Decimal:
        if asyncpg is None:
            return Decimal("0")
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            return Decimal("0")

        conn = await asyncpg.connect(dsn)
        try:
            row = await conn.fetchrow(
                f"""
                SELECT COALESCE(SUM(realized_pnl), 0) AS realized_pnl_sum
                FROM {schema}.position_snapshots
                WHERE account_id = $1
                  AND snapshot_time >= $2
                """,
                account_id,
                period_start,
            )
            if row is None:
                return Decimal("0")
            realized = _to_decimal(row["realized_pnl_sum"])
            if realized >= Decimal("0"):
                return Decimal("0")
            return -realized
        finally:
            await conn.close()

    async def _fetch_nav(self, schema: str, account_id: str) -> Decimal:
        if asyncpg is None:
            return Decimal("0")
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            return Decimal("0")

        conn = await asyncpg.connect(dsn)
        try:
            row = await conn.fetchrow(
                f"""
                SELECT cash_balance
                FROM {schema}.cash_snapshots
                WHERE account_id = $1
                ORDER BY snapshot_time DESC
                LIMIT 1
                """,
                account_id,
            )
            if row is None:
                return Decimal("0")
            return _to_decimal(row["cash_balance"])
        finally:
            await conn.close()

    async def evaluate_loss_limits(self, account_id: str, schema: str = "trading_paper") -> tuple[_LossWindow, _LossWindow, _LossWindow]:
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        nav = await self._fetch_nav(schema, account_id)
        daily_loss = await self._fetch_cumulative_realized(schema, account_id, day_start)
        weekly_loss = await self._fetch_cumulative_realized(schema, account_id, week_start)
        monthly_loss = await self._fetch_cumulative_realized(schema, account_id, month_start)

        daily_limit = nav * self.risk_profile.max_daily_loss_pct_nav
        weekly_limit = nav * self.risk_profile.max_weekly_loss_pct_nav
        monthly_limit = nav * self.risk_profile.max_monthly_loss_pct_nav

        daily = _LossWindow(loss=daily_loss, nav=nav, limit=daily_limit, period_start=day_start)
        weekly = _LossWindow(loss=weekly_loss, nav=nav, limit=weekly_limit, period_start=week_start)
        monthly = _LossWindow(loss=monthly_loss, nav=nav, limit=monthly_limit, period_start=month_start)

        if daily.limit > Decimal("0"):
            daily.used_ratio = daily.loss / daily.limit
        if weekly.limit > Decimal("0"):
            weekly.used_ratio = weekly.loss / weekly.limit
        if monthly.limit > Decimal("0"):
            monthly.used_ratio = monthly.loss / monthly.limit

        self._latest_window = {
            "daily": daily.used_ratio,
            "weekly": weekly.used_ratio,
            "monthly": monthly.used_ratio,
            "daily_pct": (daily.used_ratio * Decimal("100")),
            "weekly_pct": (weekly.used_ratio * Decimal("100")),
            "monthly_pct": (monthly.used_ratio * Decimal("100")),
        }
        return daily, weekly, monthly

    async def check(self, request: OrderRequest, account: Account | None) -> RiskCheckResult:
        if account is None:
            return RiskCheckResult(passed=False, stage="limits", reason="missing account")

        notional = request.quantity * (request.price or Decimal("0"))
        if notional > self.max_order_notional:
            return RiskCheckResult(
                passed=False,
                stage="limits",
                reason=f"order notional exceeds max {self.max_order_notional}",
            )

        if request.side.upper() == "BUY" and notional > account.cash_balance * self.max_cash_ratio:
            return RiskCheckResult(
                passed=False,
                stage="limits",
                reason="order notional exceeds available cash ratio",
            )

        environment = os.getenv("ENVIRONMENT", "paper").lower()
        daily, weekly, _ = await self.evaluate_loss_limits(
            request.account_id,
            schema=f"trading_{environment}",
        )

        if daily.limit > Decimal("0") and daily.loss >= daily.limit:
            return RiskCheckResult(
                passed=False,
                stage="limits",
                reason="daily realized loss limit reached",
                details={
                    "action": "AUTO_EMERGENCY_STOP",
                    "loss": str(daily.loss),
                    "limit": str(daily.limit),
                    "used_ratio": str(daily.used_ratio),
                },
            )

        if weekly.limit > Decimal("0") and weekly.loss >= weekly.limit:
            return RiskCheckResult(
                passed=False,
                stage="limits",
                reason="weekly realized loss limit reached",
                details={
                    "action": "ALERT_FORCE_LIVE_APPROVAL",
                    "loss": str(weekly.loss),
                    "limit": str(weekly.limit),
                    "used_ratio": str(weekly.used_ratio),
                },
            )

        return RiskCheckResult(passed=True, stage="limits")
