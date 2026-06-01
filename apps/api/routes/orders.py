"""Order trigger endpoint."""

from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter
from pydantic import BaseModel

from apps.api.state import APP_STATE
from core.models.market import Side
from core.models.order import OrderRequest

router = APIRouter()


class TriggerRequest(BaseModel):
    account_id: str = "default"
    symbol: str = "005930"
    side: str = Side.BUY.value
    quantity: Decimal = Decimal("1")
    price: Decimal | None = None
    order_type: str = "MARKET"


@router.post("/orders/trigger")
async def trigger(request: TriggerRequest) -> dict[str, object]:
    intent = OrderRequest(
        account_id=request.account_id,
        symbol=request.symbol,
        side=request.side,
        quantity=request.quantity,
        price=request.price,
        order_type=request.order_type,
    )
    result = await APP_STATE.engine.submit_order_intent(intent)
    return {
        "order_intent_id": result.order_intent_id,
        "state": result.state.value,
        "risk_passed": result.risk_check.passed,
        "risk_stage": result.risk_check.stage,
        "message": result.message,
    }
