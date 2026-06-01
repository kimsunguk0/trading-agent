"""Sanity checks before risk limits and compliance."""

from __future__ import annotations

from decimal import Decimal

from core.models.order import OrderRequest, RiskCheckResult


class SanityCheckAgent:
    async def check(self, request: OrderRequest) -> RiskCheckResult:
        if request.quantity <= Decimal("0"):
            return RiskCheckResult(
                passed=False,
                stage="sanity",
                reason="quantity must be greater than zero",
            )
        if request.order_type == "LIMIT" and (request.price is None or request.price <= Decimal("0")):
            return RiskCheckResult(
                passed=False,
                stage="sanity",
                reason="limit price must be positive",
            )
        if request.side not in {"BUY", "SELL"}:
            return RiskCheckResult(passed=False, stage="sanity", reason="invalid side")
        return RiskCheckResult(passed=True, stage="sanity")
