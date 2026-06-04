"""Kiwoom REST OpenAPI mock adapter for Korean domestic stocks."""

from __future__ import annotations

from brokers.capabilities import KIWOOM_KR_MOCK_CAPS
from brokers.kiwoom_rest_kr_base import (
    KIWOOM_MOCK_BASE_URL,
    KIWOOM_MOCK_WS_URL,
    KiwoomApiError,
    KiwoomRestKrBaseAdapter,
    _AsyncRateLimiter,
)


class KiwoomRestKrMockAdapter(KiwoomRestKrBaseAdapter):
    name = "kiwoom_rest_kr_mock"
    capabilities = KIWOOM_KR_MOCK_CAPS
    default_base_url = KIWOOM_MOCK_BASE_URL
    default_websocket_url = KIWOOM_MOCK_WS_URL


BrokerAdapter = KiwoomRestKrMockAdapter

__all__ = ["KiwoomRestKrMockAdapter", "KiwoomApiError", "_AsyncRateLimiter"]
