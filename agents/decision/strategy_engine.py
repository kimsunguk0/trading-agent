"""Rule based strategy engine with YAML-driven strategy loading and news context.

This keeps existing MVP0 opening-range behavior for backward compatibility while
adding news-breakout signal generation support and regime/liquidity gating.
"""

from __future__ import annotations

import ast
import asyncio
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from core.events.bus import RedisStreamBus
from core.events.schemas import EventType, MarketTickEvent, NewsEvent, SignalEvent
from core.models.market import Side, Symbol

try:
    from agents.meta.knowledge_base.retriever import JournalRetriever
except Exception:  # pragma: no cover
    JournalRetriever = None

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover - optional dependency in some envs
    redis = None


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, bool):
        return Decimal("1") if value else Decimal("0")
    return Decimal(str(value))


def _load_path(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    return payload


@dataclass(frozen=True)
class RegimeContext:
    current: str = "unknown"


@dataclass(frozen=True)
class OpeningRangeConfig:
    strategy_id: str
    strategy_type: str
    market: str
    symbols: tuple[str, ...]
    opening_range_minutes: int
    breakout_up_pct: Decimal
    breakout_down_pct: Decimal
    allow_short: bool
    quantity: Decimal
    order_type: str
    signal_score: Decimal


@dataclass(frozen=True)
class _NewsRegimeConfig:
    enabled: tuple[str, ...]
    disabled: tuple[str, ...]


@dataclass(frozen=True)
class _NewsExecutionConfig:
    order_type: str
    limit_price_basis: str
    max_slippage_pct: Decimal
    allow_market_order: bool


@dataclass(frozen=True)
class _NewsRiskConfig:
    max_position_pct_nav: Decimal
    max_daily_trades: int
    max_strategy_daily_loss_pct_nav: Decimal
    max_concurrent_positions: int


@dataclass(frozen=True)
class _NewsExitConfig:
    stop_loss_pct: Decimal
    take_profit: tuple[tuple[Decimal, Decimal], ...]
    time_stop_minutes: int
    trailing_activate_at_pct: Decimal
    trailing_trail_pct: Decimal


@dataclass(frozen=True)
class NewsBreakoutConfig:
    strategy_id: str
    version: int
    market: str
    description: str
    symbols: tuple[str, ...]
    liquidity_min_avg_value_20d_krw: Decimal
    regime_filter: _NewsRegimeConfig
    entry_conditions: tuple[str, ...]
    exit: _NewsExitConfig
    risk: _NewsRiskConfig
    execution: _NewsExecutionConfig
    strategy_type: str = "news_breakout"
    quantity: Decimal = Decimal("0")


@dataclass
class _NewsStrategyState:
    session_day: date | None = None
    session_open: Decimal | None = None
    intraday_high: Decimal | None = None


@dataclass
class _SymbolState:
    trading_day: date | None = None
    range_end: datetime | None = None
    range_high: Decimal | None = None
    range_low: Decimal | None = None
    emitted: bool = False


@dataclass
class _ConditionContext:
    news: dict[str, Any]
    volume: dict[str, Any]
    price: dict[str, Any]
    orderbook: dict[str, Any]


class _SafeEvaluator(ast.NodeVisitor):
    def __init__(self, context: dict[str, Any]) -> None:
        self.context = context

    def visit(self, node: ast.AST) -> Any:
        return super().visit(node)

    def visit_Expression(self, node: ast.Expression) -> Any:
        return self.visit(node.body)

    def visit_Name(self, node: ast.Name) -> Any:
        if node.id not in self.context:
            raise ValueError(f"unknown identifier: {node.id}")
        return self.context[node.id]

    def visit_Attribute(self, node: ast.Attribute) -> Any:
        base = self.visit(node.value)
        if isinstance(base, dict):
            return base[node.attr]
        raise ValueError(f"unsupported attribute lookup: {node.attr}")

    def visit_Compare(self, node: ast.Compare) -> Any:
        left = self.visit(node.left)
        value = left
        for op, right_node in zip(node.ops, node.comparators, strict=False):
            right = self.visit(right_node)
            if isinstance(op, ast.Eq):
                ok = value == right
            elif isinstance(op, ast.NotEq):
                ok = value != right
            elif isinstance(op, ast.Lt):
                ok = value < right
            elif isinstance(op, ast.LtE):
                ok = value <= right
            elif isinstance(op, ast.Gt):
                ok = value > right
            elif isinstance(op, ast.GtE):
                ok = value >= right
            else:
                raise ValueError("unsupported comparator")
            if not ok:
                return False
            value = right
        return True

    def visit_BoolOp(self, node: ast.BoolOp) -> Any:
        values = [self.visit(v) for v in node.values]
        if isinstance(node.op, ast.And):
            return all(values)
        if isinstance(node.op, ast.Or):
            return any(values)
        raise ValueError("unsupported bool operator")

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)
        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            return left / right
        if isinstance(node.op, ast.Mod):
            return left % right
        raise ValueError("unsupported binary operator")

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        value = self.visit(node.operand)
        if isinstance(node.op, ast.USub):
            return -value
        if isinstance(node.op, ast.UAdd):
            return value
        if isinstance(node.op, ast.Not):
            return not value
        raise ValueError("unsupported unary operator")

    def visit_Constant(self, node: ast.Constant) -> Any:
        if isinstance(node.value, bool):
            return bool(node.value)
        if isinstance(node.value, str):
            return node.value
        if isinstance(node.value, (int, float)):
            return _to_decimal(node.value)
        if node.value is None:
            return None
        return node.value

    def visit_List(self, node: ast.List) -> Any:
        return [self.visit(item) for item in node.elts]

    def generic_visit(self, node: ast.AST) -> Any:  # pragma: no cover - defensive
        raise ValueError("unsupported expression")


def _evaluate_condition(expression: str, context: dict[str, Any]) -> bool:
    expr = (
        expression.replace(" true", " True")
        .replace(" false", " False")
        .replace(" true)", " True)")
        .replace(" false)", " False)")
    )
    tree = ast.parse(expr, mode="eval")
    evaluator = _SafeEvaluator(context)
    return bool(evaluator.visit(tree))


def _flatten(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _flatten(v) for k, v in value.items()}
    if isinstance(value, list):
        return {_to_decimal(i): _flatten(v) for i, v in enumerate(value)} if value else []
    return value


def _load_opening_range_strategy_file(path: Path) -> OpeningRangeConfig | None:
    data = _load_path(path)
    strategy_id = data.get("strategy_id")
    if not strategy_id:
        return None

    entry = data.get("entry", {}) if isinstance(data.get("entry", {}), dict) else {}
    strategy_type = str(entry.get("strategy_type", "opening_range_breakout"))
    if strategy_type != "opening_range_breakout":
        return None

    market = str(data.get("market", "KR")).upper()
    symbols = tuple(str(item) for item in data.get("symbols", []))
    opening_range_minutes = int(entry.get("opening_range_minutes", 30))
    breakout_up_pct = _to_decimal(entry.get("breakout_up_pct", "0"))
    breakout_down_pct = _to_decimal(entry.get("breakout_down_pct", "0"))
    allow_short = bool(entry.get("allow_short", False))
    execution = data.get("execution", {}) if isinstance(data.get("execution", {}), dict) else {}
    quantity = _to_decimal(execution.get("quantity", "1"))
    order_type = str(execution.get("order_type", "MARKET"))
    signal_score = _to_decimal(data.get("signal_score", "1"))

    return OpeningRangeConfig(
        strategy_id=str(strategy_id),
        strategy_type=strategy_type,
        market=market,
        symbols=symbols,
        opening_range_minutes=opening_range_minutes,
        breakout_up_pct=breakout_up_pct,
        breakout_down_pct=breakout_down_pct,
        allow_short=allow_short,
        quantity=quantity,
        order_type=order_type,
        signal_score=signal_score,
    )


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _load_news_strategy_file(path: Path) -> NewsBreakoutConfig | None:
    data = _load_path(path)
    strategy_id = data.get("strategy_id")
    if not strategy_id:
        return None

    strategy_entry = data.get("entry", {})
    if not isinstance(strategy_entry, dict):
        return None
    strategy_type = str(strategy_entry.get("strategy_type", "news_breakout")).lower()
    if strategy_type == "opening_range_breakout":
        return None
    if "all_of" not in strategy_entry:
        return None

    universe = data.get("universe", {}) if isinstance(data.get("universe", {}), dict) else {}
    liquidity = universe.get("liquidity", {}) if isinstance(universe.get("liquidity", {}), dict) else {}
    regime = data.get("regime_filter", {}) if isinstance(data.get("regime_filter", {}), dict) else {}

    exit_cfg = data.get("exit", {}) if isinstance(data.get("exit", {}), dict) else {}
    take_profit_cfg = exit_cfg.get("take_profit", []) if isinstance(exit_cfg.get("take_profit", []), list) else []

    risk_cfg = data.get("risk", {}) if isinstance(data.get("risk", {}), dict) else {}
    execution_cfg = data.get("execution", {}) if isinstance(data.get("execution", {}), dict) else {}

    tp_pairs: tuple[tuple[Decimal, Decimal], ...] = tuple(
        (_to_decimal(item.get("pct", "0")), _to_decimal(item.get("qty_ratio", "0")))
        for item in _as_list(take_profit_cfg)
        if isinstance(item, dict)
    )

    liquidity_threshold = _to_decimal(
        liquidity.get("min_avg_value_20d_krw", liquidity.get("min_avg_volume_20d", "0"))
    )

    return NewsBreakoutConfig(
        strategy_id=strategy_id,
        version=int(data.get("version", 1)),
        market=str(data.get("market", "KR")).upper(),
        description=str(data.get("description", "")),
        symbols=tuple(str(item) for item in _as_list(data.get("symbols", []))),
        liquidity_min_avg_value_20d_krw=liquidity_threshold,
        regime_filter=_NewsRegimeConfig(
            enabled=tuple(str(item) for item in _as_list(regime.get("enabled_regimes", ()))),
            disabled=tuple(str(item) for item in _as_list(regime.get("disabled_regimes", ()))),
        ),
        entry_conditions=tuple(str(item) for item in _as_list(strategy_entry.get("all_of", ()))),
        exit=_NewsExitConfig(
            stop_loss_pct=_to_decimal(exit_cfg.get("stop_loss_pct", "0")),
            take_profit=tp_pairs,
            time_stop_minutes=int(exit_cfg.get("time_stop_minutes", 0) or 0),
            trailing_activate_at_pct=_to_decimal(
                (
                    exit_cfg.get("trailing_stop", {}).get("activate_at_pct", "0")
                    if isinstance(exit_cfg.get("trailing_stop", {}), dict)
                    else "0"
                )
            ),
            trailing_trail_pct=_to_decimal(
                exit_cfg.get("trailing_stop", {}).get("trail_pct", "0")
                if isinstance(exit_cfg.get("trailing_stop", {}), dict)
                else "0"
            ),
        ),
        risk=_NewsRiskConfig(
            max_position_pct_nav=_to_decimal(risk_cfg.get("max_position_pct_nav", "0")),
            max_daily_trades=int(risk_cfg.get("max_daily_trades", 0) or 0),
            max_strategy_daily_loss_pct_nav=_to_decimal(risk_cfg.get("max_strategy_daily_loss_pct_nav", "0")),
            max_concurrent_positions=int(risk_cfg.get("max_concurrent_positions", 0) or 0),
        ),
        execution=_NewsExecutionConfig(
            order_type=str(execution_cfg.get("order_type", "LIMIT")),
            limit_price_basis=str(execution_cfg.get("limit_price_basis", "best_ask")),
            max_slippage_pct=_to_decimal(execution_cfg.get("max_slippage_pct", "0")),
            allow_market_order=bool(execution_cfg.get("allow_market_order", False)),
        ),
        quantity=_to_decimal(execution_cfg.get("quantity", "1")),
    )


def load_opening_range_strategies(config_dir: str = "configs/strategies") -> list[OpeningRangeConfig]:
    config_path = Path(config_dir)
    strategies: list[OpeningRangeConfig] = []
    for path in sorted(config_path.glob("*.yaml")):
        try:
            cfg = _load_opening_range_strategy_file(path)
            if cfg is not None:
                strategies.append(cfg)
        except ValueError:
            continue
    return strategies


def load_news_strategies(config_dir: str = "configs/strategies") -> list[NewsBreakoutConfig]:
    config_path = Path(config_dir)
    strategies: list[NewsBreakoutConfig] = []
    for path in sorted(config_path.glob("*.yaml")):
        try:
            cfg = _load_news_strategy_file(path)
            if cfg is not None:
                strategies.append(cfg)
        except ValueError:
            continue
    return strategies


class StrategyEngine:
    """Evaluate market ticks into SignalEvent candidates."""

    def __init__(
        self,
        config_dir: str = "configs/strategies",
        account_id: str = "default",
        bus: RedisStreamBus | None = None,
        redis_url: str = "redis://localhost:6379/0",
        stream_prefix: str = "paper.events",
        news_context_ttl_minutes: int = 20,
        liquidity_ttl_seconds: int = 60 * 5,
        rag_search_top_k: int = 3,
        rag_retriever: "JournalRetriever | None" = None,
    ) -> None:
        self.account_id = account_id
        self.config_dir = config_dir
        self.bus = bus or RedisStreamBus(redis_url=redis_url, stream_prefix=stream_prefix)
        self.stream_prefix = stream_prefix
        self.news_context_ttl_minutes = news_context_ttl_minutes
        self.liquidity_ttl_seconds = liquidity_ttl_seconds

        self.opening_range_configs = load_opening_range_strategies(config_dir)
        self.news_breakout_configs = load_news_strategies(config_dir)

        self._market_state: dict[str, _SymbolState] = defaultdict(_SymbolState)
        self._news_state: dict[str, _NewsStrategyState] = defaultdict(_NewsStrategyState)
        self._strategy_active: dict[str, bool] = {cfg.strategy_id: True for cfg in self.news_breakout_configs}
        self._current_regime = RegimeContext().current
        self._latest_news_context: dict[str, dict[str, Any]] = {}
        self._liquidity_latest: dict[tuple[str, str], Decimal] = {}
        self._liquidity_updated_at: datetime | None = None
        self._regime_last_updated: datetime | None = None
        self._rag_search_top_k = rag_search_top_k
        self._rag_retriever: Any | None = rag_retriever
        if self._rag_retriever is None and JournalRetriever is not None:
            try:
                self._rag_retriever = JournalRetriever()
            except Exception:
                self._rag_retriever = None
        self._news_context_cache_cutoff = timedelta(minutes=self.news_context_ttl_minutes)
        self._redis_client = redis.from_url(redis_url, decode_responses=True) if redis is not None else None

    def is_strategy_active(self, strategy_id: str) -> bool:
        return self._strategy_active.get(strategy_id, True)

    def update_regime(self, regime: str) -> None:
        self._current_regime = str(regime)
        self._regime_last_updated = datetime.now(timezone.utc)
        for cfg in self.news_breakout_configs:
            disabled = set(cfg.regime_filter.disabled)
            enabled = set(cfg.regime_filter.enabled)
            if self._current_regime in disabled:
                self._strategy_active[cfg.strategy_id] = False
                continue
            if enabled and self._current_regime in enabled:
                self._strategy_active[cfg.strategy_id] = True

    def set_news_payload(self, payload: dict[str, Any] | NewsEvent | SignalEvent) -> None:
        if isinstance(payload, (NewsEvent, SignalEvent)):
            payload_dict = payload.model_dump(mode="json")
        else:
            payload_dict = dict(payload)

        analysis = payload_dict.get("analysis")
        if not isinstance(analysis, dict):
            analysis = payload_dict

        candidates = analysis.get("symbol_candidates")
        if not isinstance(candidates, list):
            candidates = payload_dict.get("symbol_candidates")
            if not isinstance(candidates, list):
                return

        summary = str(analysis.get("summary", ""))
        now = datetime.now(timezone.utc)
        for item in candidates:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("code", "")).strip()
            if not symbol:
                continue

            self._latest_news_context[symbol] = {
                "analysis": analysis,
                "summary": summary,
                "catalyst": _flatten(payload_dict.get("catalyst", {})),
                "bear_case": _flatten(payload_dict.get("bear_case", {})),
                "verification": _flatten(payload_dict.get("verification", {})),
                "body_hash": str(payload_dict.get("body_hash", payload_dict.get("bodyHash", ""))),
                "received_at": now,
                "symbol_name": str(item.get("name", "")),
                "source": str(payload_dict.get("source", "")),
                "news_time": payload_dict.get("event_time"),
                "candidates": candidates,
            }

    def set_liquidity_snapshot(self, strategy_id: str, symbol: str, avg_value_20d_krw: Decimal) -> None:
        self._liquidity_latest[(strategy_id, str(symbol))] = _to_decimal(avg_value_20d_krw)
        self._liquidity_updated_at = datetime.now(timezone.utc)

    def _passes_regime(self, cfg: NewsBreakoutConfig) -> bool:
        if not self.is_strategy_active(cfg.strategy_id):
            return False
        if self._current_regime in cfg.regime_filter.disabled:
            return False
        if cfg.regime_filter.enabled and self._current_regime not in cfg.regime_filter.enabled:
            return False
        return True

    def _passes_liquidity(self, cfg: NewsBreakoutConfig, symbol: str) -> bool:
        threshold = _to_decimal(cfg.liquidity_min_avg_value_20d_krw)
        if threshold <= Decimal("0"):
            return True
        avg_value = self._liquidity_latest.get((cfg.strategy_id, str(symbol)))
        if avg_value is None:
            return True
        return avg_value >= threshold

    def _market_match(self, cfg: OpeningRangeConfig | NewsBreakoutConfig, tick: MarketTickEvent) -> bool:
        if cfg.market == "ALL":
            return True
        return cfg.market == tick.market.value

    def _symbol_match(self, cfg: OpeningRangeConfig | NewsBreakoutConfig, tick: MarketTickEvent) -> bool:
        if not cfg.symbols:
            return True
        return str(tick.symbol) in cfg.symbols

    def _symbol_state(self, tick: MarketTickEvent) -> _SymbolState:
        return self._market_state[str(tick.symbol)]

    def _news_state_for_symbol(self, tick: MarketTickEvent) -> _NewsStrategyState:
        return self._news_state[str(tick.symbol)]

    def _reset_opening(self, state: _SymbolState, tick: MarketTickEvent, cfg: OpeningRangeConfig) -> None:
        day = tick.occurred_at.date()
        if state.trading_day == day:
            return
        state.trading_day = day
        state.range_end = tick.occurred_at + timedelta(minutes=cfg.opening_range_minutes)
        state.range_high = tick.price
        state.range_low = tick.price
        state.emitted = False

    def _entry_context(self, cfg: NewsBreakoutConfig, tick: MarketTickEvent, symbol: str) -> _ConditionContext:
        state = self._news_state_for_symbol(tick)
        today = tick.occurred_at.astimezone(timezone.utc).date()
        if state.session_day != today:
            state.session_day = today
            state.session_open = tick.price
            state.intraday_high = tick.price
        elif state.intraday_high is None or tick.price > state.intraday_high:
            state.intraday_high = tick.price

        analysis = self._latest_news_context.get(symbol, {})
        analysis_payload = analysis.get("analysis", {})
        if not isinstance(analysis_payload, dict):
            analysis_payload = {}

        age_minutes = Decimal("0")
        received_at = analysis.get("received_at")
        if isinstance(received_at, datetime):
            age_minutes = _to_decimal(
                max(0, int((tick.occurred_at - received_at).total_seconds() / 60))
            )

        spread_pct = Decimal("0")
        if _to_decimal(tick.ask) > Decimal("0"):
            spread_pct = (_to_decimal(tick.ask) - _to_decimal(tick.bid)) / _to_decimal(tick.ask)

        price_change = Decimal("0")
        if state.session_open and _to_decimal(state.session_open) != Decimal("0"):
            price_change = (_to_decimal(tick.price) - _to_decimal(state.session_open)) / _to_decimal(state.session_open)

        avg_value_20d = _to_decimal(cfg.liquidity_min_avg_value_20d_krw)
        cached_avg = self._liquidity_latest.get((cfg.strategy_id, symbol))
        if cached_avg is not None:
            avg_value_20d = cached_avg

        verification = analysis.get("verification", {})
        verification_passed = bool(
            isinstance(verification, dict)
            and not verification.get("duplicate_flag", False)
            and not verification.get("trap_case", False)
        )

        return _ConditionContext(
            news={
                "symbol_candidates": analysis_payload.get("symbol_candidates", []),
                "event_type": analysis_payload.get("event_type", "other"),
                "sentiment": analysis_payload.get("sentiment", "neutral"),
                "sentiment_score": _to_decimal(analysis_payload.get("sentiment_score", 0)),
                "catalyst_score": _to_decimal(analysis_payload.get("catalyst_score", 0)),
                "time_sensitivity": analysis_payload.get("time_sensitivity", "intraday"),
                "source_quality": _to_decimal(analysis_payload.get("source_quality", 0)),
                "summary": analysis.get("summary", ""),
                "age_minutes": age_minutes,
                "verification_passed": verification_passed,
            },
            volume={
                "value_1m_krw": _to_decimal(tick.volume) * _to_decimal(tick.price),
                "avg_volume": {"value_1m_20d_krw": avg_value_20d},
            },
            price={
                "breaks_intraday_high": bool(state.intraday_high and tick.price >= state.intraday_high),
                "change_from_open": price_change,
            },
            orderbook={"spread_pct": spread_pct},
        )

    def _conditions_met(self, cfg: NewsBreakoutConfig, context: _ConditionContext) -> bool:
        ctx = {
            "news": context.news,
            "volume": context.volume,
            "avg_volume": context.volume.get("avg_volume", {}),
            "price": context.price,
            "orderbook": context.orderbook,
        }
        for condition in cfg.entry_conditions:
            if not _evaluate_condition(condition, ctx):
                return False
        return True

    def _build_rag_context(
        self,
        symbol: str,
        context: _ConditionContext,
    ) -> dict[str, Any] | None:
        if self._rag_retriever is None:
            return None

        try:
            similar = self._rag_retriever.search(
                symbol=symbol,
                news_summary=str(context.news.get("summary", "")),
                regime=self._current_regime,
                top_k=self._rag_search_top_k,
            )
        except Exception:
            return None

        if not similar:
            return None

        cases: list[dict[str, Any]] = []
        total = Decimal("0")
        for item in similar:
            if not isinstance(item, dict):
                continue
            pnl_pct = _to_decimal(item.get("pnl_pct", "0"))
            cases.append(
                {
                    "symbol": str(item.get("symbol", symbol)),
                    "strategy_id": str(item.get("strategy_id", "")),
                    "narrative": str(item.get("narrative", "")),
                    "lessons": str(item.get("lessons", "")),
                    "pnl_pct": str(pnl_pct),
                }
            )
            total += pnl_pct

        if not cases:
            return None

        average = (total / Decimal(len(cases))).quantize(Decimal("0.0001"))
        return {"cases": cases, "count": len(cases), "avg_pnl_pct": average}

    def _make_news_signal(
        self,
        cfg: NewsBreakoutConfig,
        tick: MarketTickEvent,
        context: _ConditionContext,
        news_ctx: dict[str, Any],
        rag_context: dict[str, Any] | None = None,
    ) -> SignalEvent:
        tp_payload = [{"pct": tp[0], "qty_ratio": tp[1]} for tp in cfg.exit.take_profit]
        technical_summary = f"{cfg.strategy_id}: {tick.symbol} intraday ratio={context.price.get('change_from_open')}"

        news_summary = str(context.news.get("summary", ""))
        if rag_context:
            avg_pnl = _to_decimal(rag_context.get("avg_pnl_pct"))
            count = int(rag_context.get("count", len(rag_context.get("cases", []))))
            if count:
                sign = "+" if avg_pnl >= Decimal("0") else ""
                news_summary = (
                    f"{news_summary}\n"
                    f"RAG: past similar situations {count} cases avg {sign}{(avg_pnl * Decimal('100')).quantize(Decimal('0.01'))}%"
                )

        return SignalEvent(
            event_type=EventType.SIGNAL,
            strategy_id=cfg.strategy_id,
            account_id=self.account_id,
            symbol=tick.symbol,
            side=Side.BUY,
            signal_score=_to_decimal(context.news.get("sentiment_score", "0")),
            occurred_at=tick.occurred_at,
            payload={
                "event_type": "news_candidate",
                "is_news_signal": True,
                "code": str(tick.symbol),
                "regime": self._current_regime,
                "confidence": str(_to_decimal(context.news.get("catalyst_score", "0"))),
                "strategy_id": cfg.strategy_id,
                "strategy_version": cfg.version,
                "price": str(_to_decimal(tick.price)),
                "quantity": str(cfg.quantity),
                "news_summary": news_summary,
                "technical_summary": technical_summary,
                "rag_context": rag_context or {},
                "risk": {
                    "position_pct": str(cfg.risk.max_position_pct_nav * Decimal("100")),
                    "spread_pct": str(context.orderbook.get("spread_pct", Decimal("0")) * Decimal("100")),
                    "stop_loss_pct": str(cfg.exit.stop_loss_pct * Decimal("100")),
                    "take_profit": [
                        {"pct": str(tp[0] * Decimal("100")), "qty_ratio": str(tp[1])}
                        for tp in cfg.exit.take_profit
                    ],
                    "time_stop_minutes": cfg.exit.time_stop_minutes,
                    "trailing": {
                        "activate_at_pct": str(cfg.exit.trailing_activate_at_pct * Decimal("100")),
                        "trail_pct": str(cfg.exit.trailing_trail_pct * Decimal("100")),
                    },
                },
                "analysis": news_ctx.get("analysis", {}),
                "body_hash": news_ctx.get("body_hash", ""),
                "news_time": news_ctx.get("news_time"),
                "source": news_ctx.get("source", ""),
                "symbol_name": str(news_ctx.get("symbol_name", "")),
                "order_type": cfg.execution.order_type,
                "limit_price_basis": cfg.execution.limit_price_basis,
                "allow_market_order": bool(cfg.execution.allow_market_order),
                "execution": {
                    "order_type": cfg.execution.order_type,
                    "limit_price_basis": cfg.execution.limit_price_basis,
                    "max_slippage_pct": str(cfg.execution.max_slippage_pct),
                    "allow_market_order": bool(cfg.execution.allow_market_order),
                },
            },
        )

    def _make_opening_signal(self, cfg: OpeningRangeConfig, tick: MarketTickEvent, side: Side) -> SignalEvent:
        return SignalEvent(
            event_type=EventType.SIGNAL,
            strategy_id=cfg.strategy_id,
            account_id=self.account_id,
            symbol=tick.symbol,
            side=side,
            signal_score=cfg.signal_score,
            occurred_at=tick.occurred_at,
            payload={
                "event_type": "opening_range",
                "is_news_signal": False,
                "regime": self._current_regime,
                "strategy_id": cfg.strategy_id,
                "order_type": cfg.order_type,
                "quantity": str(cfg.quantity),
            },
        )

    def _evaluate_opening(self, tick: MarketTickEvent) -> list[SignalEvent]:
        signals: list[SignalEvent] = []
        for cfg in self.opening_range_configs:
            if not self._market_match(cfg, tick):
                continue
            if not self._symbol_match(cfg, tick):
                continue

            state = self._market_state[str(tick.symbol)]
            self._reset_opening(state, tick, cfg)

            if state.range_high is None or state.range_low is None:
                state.range_high = tick.price
                state.range_low = tick.price

            if state.range_end is not None and tick.occurred_at <= state.range_end:
                if _to_decimal(state.range_high) < tick.price:
                    state.range_high = tick.price
                if _to_decimal(state.range_low) > tick.price:
                    state.range_low = tick.price
                continue

            if state.emitted:
                continue

            up_trigger = False
            down_trigger = False
            if state.range_high is not None:
                up_trigger = tick.price >= _to_decimal(state.range_high) * (Decimal("1") + cfg.breakout_up_pct)
            if cfg.allow_short and state.range_low is not None:
                down_trigger = tick.price <= _to_decimal(state.range_low) * (Decimal("1") - cfg.breakout_down_pct)

            if up_trigger:
                state.emitted = True
                signals.append(self._make_opening_signal(cfg, tick, Side.BUY))
                continue
            if down_trigger:
                state.emitted = True
                signals.append(self._make_opening_signal(cfg, tick, Side.SELL))

        return signals

    def _evaluate_news(self, tick: MarketTickEvent) -> list[SignalEvent]:
        symbol_key = str(tick.symbol)
        symbol_state = self._news_state_for_symbol(tick)
        if symbol_state.session_day != tick.occurred_at.date():
            symbol_state.session_day = tick.occurred_at.date()
            symbol_state.session_open = tick.price
            symbol_state.intraday_high = tick.price
        elif symbol_state.intraday_high is None or tick.price > symbol_state.intraday_high:
            symbol_state.intraday_high = tick.price

        signals: list[SignalEvent] = []
        for cfg in self.news_breakout_configs:
            if not self._market_match(cfg, tick):
                continue
            if not self._symbol_match(cfg, tick):
                continue
            if not self._passes_regime(cfg):
                continue
            if not self._passes_liquidity(cfg, symbol_key):
                continue

            news_ctx = self._latest_news_context.get(symbol_key)
            if not news_ctx:
                continue
            if isinstance(news_ctx.get("received_at"), datetime):
                if datetime.now(timezone.utc) - news_ctx["received_at"] > self._news_context_cache_cutoff:
                    continue

            context = self._entry_context(cfg, tick, symbol_key)
            if not self._conditions_met(cfg, context):
                continue

            rag_context = self._build_rag_context(
                symbol=symbol_key,
                context=context,
            )
            signals.append(self._make_news_signal(cfg, tick, context, news_ctx, rag_context))

        return signals

    def evaluate(self, tick: MarketTickEvent) -> list[SignalEvent]:
        signals = self._evaluate_opening(tick)
        signals.extend(self._evaluate_news(tick))
        return signals

    async def _consume_regime(self) -> None:
        if self._redis_client is None:
            return

        stream = self.bus.stream_name("regime")
        last_id = "$"
        while True:
            messages = await self._redis_client.xread({stream: last_id}, count=16, block=2000)
            if not messages:
                continue
            for _, entries in messages:
                for message_id, payloads in entries:
                    last_id = message_id
                    raw = payloads.get("payload") if isinstance(payloads, dict) else None
                    if not isinstance(raw, str):
                        continue
                    try:
                        data = yaml.safe_load(raw)
                    except Exception:
                        import json

                        try:
                            data = json.loads(raw)
                        except Exception:
                            data = {}
                    if not isinstance(data, dict):
                        continue
                    regime = data.get("regime") or data.get("state") or data.get("value")
                    if isinstance(regime, str) and regime:
                        self.update_regime(regime)

    async def _consume_news_signals(self) -> None:
        if self._redis_client is None:
            return
        stream = self.bus.stream_name("signals")
        last_id = "$"
        while True:
            messages = await self._redis_client.xread({stream: last_id}, count=16, block=2000)
            if not messages:
                continue
            for _, entries in messages:
                for message_id, payloads in entries:
                    last_id = message_id
                    raw = payloads.get("payload") if isinstance(payloads, dict) else None
                    if not isinstance(raw, str):
                        continue
                    try:
                        body = yaml.safe_load(raw)
                    except Exception:
                        import json

                        try:
                            body = json.loads(raw)
                        except Exception:
                            body = {}
                    if not isinstance(body, dict):
                        continue

                    kind = body.get("event_type") or body.get("event")
                    if kind in {"news_context", "news", "news_signal", "news_analysis"}:
                        self.set_news_payload(body)

    async def _consume_market_ticks(self) -> None:
        async for event in self.bus.subscribe("market_tick"):
            if not isinstance(event, MarketTickEvent):
                continue
            for signal in self.evaluate(event):
                await self.bus.publish(signal)

    async def run(self) -> None:
        if self._redis_client is None:
            # legacy fallback stream only.
            await self._consume_market_ticks()
            return
        await asyncio.gather(
            self._consume_market_ticks(),
            self._consume_news_signals(),
            self._consume_regime(),
        )


async def main() -> None:
    env = os.getenv("ENVIRONMENT", "paper")
    StreamPrefix = f"{env}.events"
    engine = StrategyEngine(stream_prefix=StreamPrefix)
    await engine.run()


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
