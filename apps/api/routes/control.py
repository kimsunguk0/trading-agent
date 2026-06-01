"""Control endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from apps.api.state import APP_STATE

router = APIRouter()


@router.post("/halt")
def halt() -> dict[str, str]:
    APP_STATE.system_state.halt("user_requested")
    return {"state": APP_STATE.system_state.state.value}
