from __future__ import annotations

from decimal import Decimal

from core.models.order import OrderRequest
from core.models.portfolio import Account
from core.risk.gate import RiskGate


class _ManualRiskAccount:
    def __init__(self) -> None:
        self.account = Account(account_id="acc", cash_balance=Decimal("100"))


async def test_risk_gate_blocks_invalid_side() -> None:
    gate = RiskGate()
    request = OrderRequest(account_id="acc", symbol="005930", side="HOLD", quantity=Decimal("1"))
    result = await gate.evaluate(request, Account(account_id="acc", cash_balance=Decimal("100")))
    assert not result.passed
    assert result.stage == "sanity"


async def test_risk_gate_blocks_limit_price_missing() -> None:
    gate = RiskGate()
    request = OrderRequest(account_id="acc", symbol="005930", side="BUY", quantity=Decimal("1"), order_type="LIMIT", price=None)
    result = await gate.evaluate(request, Account(account_id="acc", cash_balance=Decimal("1000")))
    assert not result.passed
    assert result.stage == "sanity"


async def test_risk_gate_blocks_over_notional() -> None:
    gate = RiskGate()
    request = OrderRequest(account_id="acc", symbol="005930", side="BUY", quantity=Decimal("1000000"), price=Decimal("100"))
    result = await gate.evaluate(request, Account(account_id="acc", cash_balance=Decimal("2000000000")))
    assert not result.passed
    assert result.stage == "limits"
