"""Kiwoom REST OpenAPI live adapter for Korean domestic stocks."""

from __future__ import annotations

import os

from brokers.capabilities import KIWOOM_KR_LIVE_CAPS
from brokers.kiwoom_rest_kr_base import (
    KIWOOM_LIVE_BASE_URL,
    KIWOOM_LIVE_WS_URL,
    KiwoomApiError,
    KiwoomRestKrBaseAdapter,
    _AsyncRateLimiter,
    _parse_price,
)


class KiwoomRestKrLiveAdapter(KiwoomRestKrBaseAdapter):
    name = "kiwoom_rest_kr_live"
    capabilities = KIWOOM_KR_LIVE_CAPS
    default_base_url = KIWOOM_LIVE_BASE_URL
    default_websocket_url = KIWOOM_LIVE_WS_URL

    def __init__(
        self,
        app_key: str | None = None,
        app_secret: str | None = None,
        account_no: str | None = None,
        base_url: str | None = None,
        websocket_url: str | None = None,
        rate_limit: int = 5,
        transport=None,
    ) -> None:
        super().__init__(
            app_key=app_key or os.getenv("KIWOOM_LIVE_APP_KEY") or os.getenv("KIWOOM_APP_KEY"),
            app_secret=app_secret or os.getenv("KIWOOM_LIVE_APP_SECRET") or os.getenv("KIWOOM_APP_SECRET"),
            account_no=account_no or os.getenv("KIWOOM_LIVE_ACCOUNT_NO") or os.getenv("KIWOOM_ACCOUNT_NO"),
            base_url=base_url,
            websocket_url=websocket_url,
            rate_limit=rate_limit,
            transport=transport,
        )


BrokerAdapter = KiwoomRestKrLiveAdapter

__all__ = ["KiwoomRestKrLiveAdapter", "KiwoomApiError", "_AsyncRateLimiter", "_parse_price"]
