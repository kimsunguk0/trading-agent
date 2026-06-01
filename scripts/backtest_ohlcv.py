"""Simple ORB OHLCV backtest with look-ahead filter and fee/tax handling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any

import asyncpg
import yaml

getcontext().prec = 28


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(timezone.utc)
    return datetime.now(timezone.utc)


@dataclass
class OhlcvBar:
    bucket_start: datetime
    market: str
    symbol: str
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal


@dataclass
class Trade:
    symbol: str
    entry_time: datetime
    exit_time: datetime | None
    entry_price: Decimal
    exit_price: Decimal | None
    quantity: Decimal
    pnl: Decimal


@dataclass
class StrategyParams:
    opening_range_minutes: int = 30
    breakout_up_pct: Decimal = Decimal("0.0025")
    stop_loss_pct: Decimal = Decimal("0.02")
    take_profit_pct: Decimal = Decimal("0.04")
    end_of_day_exit: bool = True
    quantity: Decimal = Decimal("1")


@dataclass
class SimState:
    trading_day: datetime.date | None = None
    range_end: datetime | None = None
    range_high: Decimal = Decimal("0")
    range_low: Decimal = Decimal("0")
    emitted: bool = False
    entry_price: Decimal | None = None
    entry_time: datetime | None = None
    qty: Decimal | None = None
    entry_fee: Decimal = Decimal("0")


def _minute_delta(minutes: int) -> timedelta:
    return timedelta(minutes=minutes)


def _coerce_row(row: dict[str, Any]) -> OhlcvBar:
    return OhlcvBar(
        bucket_start=_as_datetime(row.get("bucket_start") or row.get("time") or row.get("ts")),
        market=str(row.get("market", "KR")),
        symbol=str(row.get("symbol")),
        open_price=_to_decimal(row.get("open_price", row.get("open"))),
        high_price=_to_decimal(row.get("high_price", row.get("high"))),
        low_price=_to_decimal(row.get("low_price", row.get("low"))),
        close_price=_to_decimal(row.get("close_price", row.get("close"))),
        volume=_to_decimal(row.get("volume", "0")),
    )


def _load_fees(path: str) -> dict[str, Decimal]:
    payload = yaml.safe_load(Path(path).read_text()) or {}
    if not isinstance(payload, dict):
        payload = {}
    return {
        "taker_fee_bps": _to_decimal(payload.get("taker_fee_bps", "0")),
        "maker_fee_bps": _to_decimal(payload.get("maker_fee_bps", "0")),
        "trade_tax_bps": _to_decimal(payload.get("trade_tax_bps", "0")),
    }


def _load_strategy(path: str) -> StrategyParams:
    payload = yaml.safe_load(Path(path).read_text()) or {}
    if not isinstance(payload, dict):
        return StrategyParams()

    entry = payload.get("entry", {}) if isinstance(payload.get("entry", {}), dict) else {}
    exit_cfg = payload.get("exit", {}) if isinstance(payload.get("exit", {}), dict) else {}

    return StrategyParams(
        opening_range_minutes=int(entry.get("opening_range_minutes", 30)),
        breakout_up_pct=_to_decimal(entry.get("breakout_up_pct", "0.0025")),
        stop_loss_pct=_to_decimal(exit_cfg.get("stop_loss_pct", "0.02")),
        take_profit_pct=_to_decimal(exit_cfg.get("take_profit_pct", "0.04")),
        end_of_day_exit=bool(exit_cfg.get("end_of_day_exit", True)),
        quantity=_to_decimal(payload.get("execution", {}).get("quantity", "1")),
    )


async def _load_db_bars(
    dsn: str,
    schema: str,
    symbol: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[OhlcvBar]:
    conn = await asyncpg.connect(dsn)
    try:
        conditions: list[str] = []
        values: list[Any] = []
        idx = 1

        if symbol is not None:
            conditions.append(f"symbol = ${idx}")
            values.append(symbol)
            idx += 1
        if start is not None:
            conditions.append(f"bucket_start >= ${idx}")
            values.append(start)
            idx += 1
        if end is not None:
            conditions.append(f"bucket_start <= ${idx}")
            values.append(end)
            idx += 1

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        rows = await conn.fetch(
            f"""
            SELECT bucket_start, market, symbol, open_price, high_price, low_price, close_price, volume
            FROM {schema}.ohlcv
            {where}
            ORDER BY bucket_start ASC
            """,
            *values,
        )

        bars: list[OhlcvBar] = []
        for row in rows:
            bars.append(
                OhlcvBar(
                    bucket_start=row["bucket_start"],
                    market=row["market"],
                    symbol=row["symbol"],
                    open_price=row["open_price"],
                    high_price=row["high_price"],
                    low_price=row["low_price"],
                    close_price=row["close_price"],
                    volume=row["volume"],
                )
            )
        return bars
    finally:
        await conn.close()


def _exit_position(
    bar: OhlcvBar,
    state: SimState,
    fees: dict[str, Decimal],
    trades: list[Trade],
    cash: Decimal,
) -> tuple[Decimal, bool]:
    if state.entry_price is None or state.qty is None or state.entry_time is None:
        return cash, False

    exit_notional = state.qty * bar.close_price
    sell_fee = exit_notional * fees["maker_fee_bps"] / Decimal("10000")
    trade_tax = exit_notional * fees["trade_tax_bps"] / Decimal("10000")
    pnl = (bar.close_price - state.entry_price) * state.qty - state.entry_fee - sell_fee - trade_tax

    trades.append(
        Trade(
            symbol=bar.symbol,
            entry_time=state.entry_time,
            exit_time=bar.bucket_start,
            entry_price=state.entry_price,
            exit_price=bar.close_price,
            quantity=state.qty,
            pnl=pnl,
        )
    )

    cash += exit_notional - sell_fee - trade_tax
    state.entry_price = None
    state.entry_time = None
    state.qty = None
    state.entry_fee = Decimal("0")
    state.emitted = False
    state.range_end = None
    return cash, True


def _simulate(
    bars: list[OhlcvBar],
    params: StrategyParams,
    fees: dict[str, Decimal],
    initial_cash: Decimal,
) -> tuple[Decimal, list[Trade], list[tuple[datetime, Decimal]]]:
    state_by_symbol: dict[str, SimState] = {}
    last_price_by_symbol: dict[str, Decimal] = {}
    last_bar_by_symbol: dict[str, OhlcvBar] = {}

    cash = initial_cash
    trades: list[Trade] = []
    equity_curve: list[tuple[datetime, Decimal]] = []

    def mark_to_market(total_cash: Decimal, as_of: datetime) -> Decimal:
        equity = total_cash
        for symbol, state in state_by_symbol.items():
            if state.entry_price is None or state.qty is None:
                continue
            last_price = last_price_by_symbol.get(symbol, state.entry_price)
            equity += state.qty * last_price
        return equity

    for bar in bars:
        state = state_by_symbol.setdefault(
            bar.symbol,
            SimState(range_high=bar.close_price, range_low=bar.close_price),
        )
        bar_day = bar.bucket_start.date()
        prev_bar = last_bar_by_symbol.get(bar.symbol)

        if state.trading_day != bar_day:
            if params.end_of_day_exit and state.entry_price is not None and prev_bar is not None:
                cash, _ = _exit_position(prev_bar, state, fees, trades, cash)

            state.trading_day = bar_day
            state.range_end = bar.bucket_start + _minute_delta(params.opening_range_minutes)
            state.range_high = bar.close_price
            state.range_low = bar.close_price
            state.emitted = False

        if state.range_end is None:
            state.range_end = bar.bucket_start + _minute_delta(params.opening_range_minutes)

        in_range_window = bar.bucket_start <= state.range_end
        if in_range_window:
            if bar.high_price > state.range_high:
                state.range_high = bar.high_price
            if bar.low_price < state.range_low:
                state.range_low = bar.low_price
            last_price_by_symbol[bar.symbol] = bar.close_price
            last_bar_by_symbol[bar.symbol] = bar
            equity_curve.append((bar.bucket_start, mark_to_market(cash, bar.bucket_start)))
            continue

        if state.entry_price is not None:
            stop_level = state.entry_price * (Decimal("1") - params.stop_loss_pct)
            take_level = state.entry_price * (Decimal("1") + params.take_profit_pct)

            if params.stop_loss_pct > Decimal("0") and bar.close_price <= stop_level:
                cash, _ = _exit_position(bar, state, fees, trades, cash)
            elif params.take_profit_pct > Decimal("0") and bar.close_price >= take_level:
                cash, _ = _exit_position(bar, state, fees, trades, cash)

            last_price_by_symbol[bar.symbol] = bar.close_price
            last_bar_by_symbol[bar.symbol] = bar
            equity_curve.append((bar.bucket_start, mark_to_market(cash, bar.bucket_start)))
            continue

        breakout = state.range_high * (Decimal("1") + params.breakout_up_pct)
        if (not state.emitted) and bar.close_price >= breakout:
            order_notional = params.quantity * bar.close_price
            buy_fee = order_notional * fees["taker_fee_bps"] / Decimal("10000")
            if cash >= order_notional + buy_fee:
                state.entry_price = bar.close_price
                state.entry_time = bar.bucket_start
                state.qty = params.quantity
                state.entry_fee = buy_fee
                state.emitted = True
                cash -= order_notional + buy_fee

        last_price_by_symbol[bar.symbol] = bar.close_price
        last_bar_by_symbol[bar.symbol] = bar
        equity_curve.append((bar.bucket_start, mark_to_market(cash, bar.bucket_start)))

    if params.end_of_day_exit:
        for symbol, state in list(state_by_symbol.items()):
            if state.entry_price is None:
                continue
            last_bar = last_bar_by_symbol.get(symbol)
            if last_bar is None:
                continue
            cash, _ = _exit_position(last_bar, state, fees, trades, cash)

    return cash, trades, equity_curve


def _metrics(
    initial_cash: Decimal,
    final_cash: Decimal,
    trades: list[Trade],
    equity_curve: list[tuple[datetime, Decimal]],
) -> dict[str, Decimal]:
    cumulative_return = (final_cash - initial_cash) / initial_cash

    if trades:
        winners = sum(1 for trade in trades if trade.pnl > 0)
        win_rate = Decimal(winners) / Decimal(len(trades))
        gross_profit = sum((trade.pnl for trade in trades if trade.pnl > 0), Decimal("0"))
        gross_loss = sum((abs(trade.pnl) for trade in trades if trade.pnl < 0), Decimal("0"))
        profit_factor = gross_profit / gross_loss if gross_loss > Decimal("0") else Decimal("Infinity")
    else:
        win_rate = Decimal("0")
        profit_factor = Decimal("0")

    daily_equity: dict[datetime.date, Decimal] = {}
    for ts, equity in equity_curve:
        daily_equity[ts.date()] = equity

    daily_returns: list[Decimal] = []
    ordered_days = sorted(daily_equity)
    for idx in range(1, len(ordered_days)):
        prev = daily_equity[ordered_days[idx - 1]]
        cur = daily_equity[ordered_days[idx]]
        if prev > Decimal("0"):
            daily_returns.append((cur / prev) - Decimal("1"))

    if len(daily_returns) > 1:
        avg = sum(daily_returns, Decimal("0")) / Decimal(len(daily_returns))
        variance = sum((value - avg) ** 2 for value in daily_returns) / Decimal(len(daily_returns))
        stdev = variance.sqrt()
        sharpe_ratio = (avg / stdev) * Decimal("252").sqrt() if stdev > Decimal("0") else Decimal("0")
    else:
        sharpe_ratio = Decimal("0")

    max_drawdown = Decimal("0")
    peak = Decimal("0")
    for _, equity in equity_curve:
        if equity > peak:
            peak = equity
        if peak > Decimal("0"):
            dd = (peak - equity) / peak
            if dd > max_drawdown:
                max_drawdown = dd

    return {
        "cumulative_return": cumulative_return,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown": max_drawdown,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
    }


async def run_backtest(
    ohlcv_rows: list[dict[str, Any]] | None = None,
    *,
    database_url: str | None = None,
    schema: str = "trading_paper",
    symbol: str | None = None,
    as_of: datetime | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    strategy_config: str = "configs/strategies/kr_opening_range_breakout.yaml",
    fees_config: str = "configs/fees/kr_2026.yaml",
    initial_cash: Decimal = Decimal("1000000"),
) -> dict[str, Decimal]:
    if ohlcv_rows is None:
        if not database_url:
            raise ValueError("either ohlcv_rows or database_url must be provided")
        bars = await _load_db_bars(database_url, schema, symbol=symbol, start=start, end=end)
    else:
        bars = [_coerce_row(item) for item in ohlcv_rows]

    if as_of is not None:
        bars = [bar for bar in bars if bar.bucket_start <= as_of]
    if start is not None:
        bars = [bar for bar in bars if bar.bucket_start >= start]
    if end is not None:
        bars = [bar for bar in bars if bar.bucket_start <= end]

    bars.sort(key=lambda item: item.bucket_start)
    if not bars:
        return {
            "cumulative_return": Decimal("0"),
            "sharpe_ratio": Decimal("0"),
            "max_drawdown": Decimal("0"),
            "win_rate": Decimal("0"),
            "profit_factor": Decimal("0"),
        }

    params = _load_strategy(strategy_config)
    fees = _load_fees(fees_config)
    final_cash, trades, equity_curve = _simulate(bars, params, fees, initial_cash)
    return _metrics(initial_cash, final_cash, trades, equity_curve)

