from __future__ import annotations

import re
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from agents.decision.decision_engine import DecisionPolicyEngine
from agents.decision.strategy_engine import EventType, MarketTickEvent, StrategyEngine, load_opening_range_strategies
from core.models.market import Market, Symbol


def test_load_opening_range_strategy_from_yaml(tmp_path) -> None:
    cfg = tmp_path / "orb.yaml"
    cfg.write_text(
        """
strategy_id: orb_test
market: KR
symbols:
  - "005930"
entry:
  strategy_type: opening_range_breakout
  opening_range_minutes: 20
  breakout_up_pct: 0.005
  breakout_down_pct: 0.004
  allow_short: false
execution:
  order_type: MARKET
  quantity: 2
signal_score: 1.25
""",
        encoding="utf-8",
    )

    strategies = load_opening_range_strategies(str(tmp_path))
    assert len(strategies) == 1
    cfg_obj = strategies[0]
    assert cfg_obj.strategy_id == "orb_test"
    assert cfg_obj.market == "KR"
    assert cfg_obj.opening_range_minutes == 20
    assert cfg_obj.breakout_up_pct == Decimal("0.005")
    assert cfg_obj.breakout_down_pct == Decimal("0.004")
    assert cfg_obj.quantity == Decimal("2")


def _make_tick(symbol: str, price: str, occurred_at: datetime) -> MarketTickEvent:
    return MarketTickEvent(
        event_type=EventType.MARKET_TICK,
        symbol=Symbol(symbol),
        market=Market.KR,
        bid=Decimal(price),
        ask=Decimal(price),
        price=Decimal(price),
        volume=Decimal("0"),
        occurred_at=occurred_at,
    )


def test_entry_condition_generates_signal_on_breakout(tmp_path) -> None:
    cfg = tmp_path / "orb.yaml"
    cfg.write_text(
        """
strategy_id: orb_signal
market: KR
symbols:
  - "005930"
entry:
  strategy_type: opening_range_breakout
  opening_range_minutes: 30
  breakout_up_pct: 0.01
  breakout_down_pct: 0.01
execution:
  order_type: MARKET
  quantity: 1
""",
        encoding="utf-8",
    )

    engine = StrategyEngine(config_dir=str(tmp_path), account_id="acc")
    base = datetime(2026, 5, 29, 9, 0, 0, tzinfo=timezone.utc)

    first = _make_tick("005930", "100", base)
    second = _make_tick("005930", "105", base + timedelta(minutes=10))
    third = _make_tick("005930", "120", base + timedelta(minutes=40))

    assert engine.evaluate(first) == []
    assert engine.evaluate(second) == []
    signals = engine.evaluate(third)
    assert len(signals) == 1
    assert str(signals[0].symbol) == "005930"
    assert signals[0].side.value == "BUY"


def test_entry_condition_not_triggered_when_no_breakout(tmp_path) -> None:
    cfg = tmp_path / "orb.yaml"
    cfg.write_text(
        """
strategy_id: orb_signal
market: KR
symbols:
  - "005930"
entry:
  strategy_type: opening_range_breakout
  opening_range_minutes: 30
  breakout_up_pct: 0.20
  breakout_down_pct: 0.20
execution:
  order_type: MARKET
  quantity: 1
""",
        encoding="utf-8",
    )

    engine = StrategyEngine(config_dir=str(tmp_path), account_id="acc")
    base = datetime(2026, 5, 29, 9, 0, 0, tzinfo=timezone.utc)

    in_window = _make_tick("005930", "100", base + timedelta(minutes=10))
    out_window = _make_tick("005930", "130", base + timedelta(minutes=40))

    assert engine.evaluate(in_window) == []
    assert engine.evaluate(out_window) == []


def test_decision_policy_engine_order_intent_id_format() -> None:
    policy = DecisionPolicyEngine(
        account_id="acc",
        date_fn=lambda: datetime(2026, 5, 29, 9, 0, 0, tzinfo=timezone.utc),
    )

    oid = policy.next_order_intent_id("KR", "005930")
    assert re.fullmatch(r"OI-\d{8}-KR-005930-\d{4}", oid)

    oid2 = policy.next_order_intent_id("KR", "005930")
    assert oid2.endswith("-0002")
