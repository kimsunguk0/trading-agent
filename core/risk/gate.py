"""3-stage Risk Gate."""

from __future__ import annotations

from core.models.order import OrderRequest, RiskCheckResult
from core.models.portfolio import Account
from core.risk.limits import RiskManager
from core.risk.sanity_check import SanityCheckAgent
from core.risk.compliance import ComplianceAgent


class RiskGate:
    def __init__(
        self,
        sanity_check: SanityCheckAgent | None = None,
        risk_manager: RiskManager | None = None,
        compliance: ComplianceAgent | None = None,
    ) -> None:
        self.sanity_check = sanity_check or SanityCheckAgent()
        self.risk_manager = risk_manager or RiskManager()
        self.compliance = compliance or ComplianceAgent()

    async def evaluate(self, request: OrderRequest, account: Account | None) -> RiskCheckResult:
        sanity = await self.sanity_check.check(request)
        if not sanity.passed:
            return sanity

        limits = await self.risk_manager.check(request, account)
        if not limits.passed:
            return limits

        compliance = await self.compliance.check(request)
        if not compliance.passed:
            return compliance

        return RiskCheckResult(passed=True, stage="final")
