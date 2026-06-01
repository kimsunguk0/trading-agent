"""FastAPI app for MVP0 control endpoints."""

from __future__ import annotations

import logging

from fastapi import FastAPI

from apps.api.routes import control, orders, status
from apps.api.state import APP_STATE
try:
    from core.bootstrap import boot_or_raise
except Exception:  # pragma: no cover - optional import fallback
    boot_or_raise = None


logger = logging.getLogger(__name__)


app = FastAPI(title="stock-mvp0", version="0.0.0")


@app.on_event("startup")
def _startup() -> None:
    if boot_or_raise is None:
        raise RuntimeError("bootstrap hook is unavailable")
    boot_or_raise()


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "state": APP_STATE.system_state.state.value,
        "operating_mode": "READ_ONLY",
    }


app.include_router(status.router)
app.include_router(orders.router)
app.include_router(control.router)
