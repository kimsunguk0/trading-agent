from __future__ import annotations

import json
from decimal import Decimal

import pytest

from agents.monitoring import slippage_monitor


class _FakeRedis:
    async def xrevrange(self, stream: str, count: int):
        return [
            (
                "1-0",
                {
                    "payload": json.dumps(
                        {
                            "event_type": "news_candidate",
                            "order_intent_id": "OI-1",
                            "strategy_id": "strat",
                            "code": "005930",
                            "price": "100",
                            "quantity": "2",
                            "execution": {"max_slippage_pct": "0.3"},
                        }
                    )
                },
            )
        ]

    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_fetch_candidate_parses_xrevrange_records(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(slippage_monitor.redis, "from_url", lambda *args, **kwargs: _FakeRedis())
    monitor = slippage_monitor.SlippageMonitor(
        stream_prefix="paper.events",
        schema="trading_paper",
        redis_url="redis://example",
    )

    candidate = await monitor._fetch_candidate("OI-1")

    assert candidate is not None
    assert candidate.symbol_code == "005930"
    assert candidate.quantity == Decimal("2")
    assert candidate.max_slippage_pct == Decimal("0.3")
