"""Status endpoint."""

from __future__ import annotations

from decimal import Decimal
from fastapi import APIRouter

from apps.api.state import APP_STATE

router = APIRouter()


@router.get("/status")
async def get_status() -> dict[str, Decimal | str]:
    snapshot = await APP_STATE.broker.get_cash_snapshot("default")
    return {
        "account_id": snapshot.account_id,
        "cash_balance": snapshot.cash_balance,
        "available_cash": snapshot.available_cash,
        "system_state": APP_STATE.system_state.state.value,
    }
