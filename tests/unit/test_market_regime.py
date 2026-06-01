from __future__ import annotations

from decimal import Decimal

from agents.context.market_regime import classify_regime_from_ohlcv, _load_regime_config
from agents.decision.strategy_engine import StrategyEngine


def _row(close: str, high: str, low: str) -> dict[str, Decimal]:
    return {
        "close_price": Decimal(close),
        "high_price": Decimal(high),
        "low_price": Decimal(low),
    }


def test_bull_trend_classification() -> None:
    cfg = _load_regime_config()
    rows = [
        _row("98", "99", "97"),
        _row("99", "100", "98"),
        _row("101", "102", "100"),
        _row("103", "104", "102"),
        _row("106", "107", "105"),
    ]
    assert classify_regime_from_ohlcv(rows, cfg) == "bull_trend"


def test_bear_trend_classification() -> None:
    cfg = _load_regime_config()
    rows = [
        _row("103", "104", "102"),
        _row("102", "103", "101"),
        _row("101", "102", "100"),
        _row("99", "100", "98"),
        _row("95", "96", "94"),
    ]
    assert classify_regime_from_ohlcv(rows, cfg) == "bear_trend"


def test_panic_classification() -> None:
    cfg = _load_regime_config()
    rows = [
        _row("100", "130", "95"),
        _row("102", "132", "90"),
        _row("98", "140", "88"),
        _row("105", "145", "90"),
        _row("103", "150", "85"),
    ]
    assert classify_regime_from_ohlcv(rows, cfg) == "panic"


def test_strategy_deactivated_in_disabled_regime(tmp_path) -> None:
    path = tmp_path / "kr_news_breakout_v1.yaml"
    path.write_text(
        """
strategy_id: test_strategy
market: KR
regime_filter:
  enabled_regimes: [bull_trend, bull_volatile, range]
  disabled_regimes: [bear_trend, panic]
entry:
  all_of:
    - news.sentiment_score >= 0.1
risk:
  max_position_pct_nav: 0.01
  max_daily_trades: 2
  max_strategy_daily_loss_pct_nav: 0.01
  max_concurrent_positions: 1
execution:
  order_type: limit
  limit_price_basis: best_ask
  max_slippage_pct: 0.001
  allow_market_order: false
""",
        encoding="utf-8",
    )
    engine = StrategyEngine(config_dir=str(tmp_path), account_id="default")
    assert engine.is_strategy_active("test_strategy") is True

    engine.update_regime("panic")
    assert engine.is_strategy_active("test_strategy") is False

    engine.update_regime("bull_trend")
    assert engine.is_strategy_active("test_strategy") is True
