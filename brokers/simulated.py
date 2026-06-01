"""Simulated broker that mimics delay, timeout, partial fill and UNKNOWN_SUBMITTED."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict

from core.models.market import Symbol
from core.models.order import Fill, OrderAck, OrderRequest, OrderStatus
from core.models.portfolio import Account, CashSnapshot, Position
from brokers.base import BrokerCapabilities
from brokers.capabilities import SIMULATED_CAPS


@dataclass
class _SimAccountState:
    account: Account


class SimulatedBrokerAdapter:
    def __init__(
        self,
        environment: str = "paper",
        initial_cash: Decimal = Decimal("1000000"),
        symbols: tuple[Symbol, ...] = (Symbol.SAMSUNG, Symbol.LG, Symbol.SKHYNIX),
    ) -> None:
        self.environment = environment
        self.capabilities = SIMULATED_CAPS
        self._accounts: Dict[str, _SimAccountState] = {}
        self._orders: Dict[str, OrderAck] = {}
        self._positions: Dict[tuple[str, str], Position] = {}
        self._prices: Dict[str, Decimal] = {
            s.value: Decimal("50000") for s in symbols
        }
        self._initial_cash = initial_cash
        self.timeout_probability = Decimal("0.05")
        self.delay_probability = Decimal("0.20")
        self.partial_fill_probability = Decimal("0.10")

    def _account(self, account_id: str) -> _SimAccountState:
        if account_id not in self._accounts:
            self._accounts[account_id] = _SimAccountState(
                account=Account(
                    account_id=account_id,
                    cash_balance=self._initial_cash,
                    currency="KRW",
                )
            )
        return self._accounts[account_id]

    def _next_price(self, symbol: str) -> Decimal:
        if symbol not in self._prices:
            self._prices[symbol] = Decimal("10000")
        current = self._prices[symbol]
        drift = Decimal(str(random.uniform(-0.002, 0.002)))
        next_price = (current * (Decimal("1") + drift)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        if next_price <= Decimal("0"):
            next_price = Decimal("100")
        self._prices[symbol] = next_price
        return next_price

    def get_account(self, account_id: str) -> Account | None:
        return self._account(account_id).account

    async def submit_order(self, request: OrderRequest) -> OrderAck | None:
        # idempotency: return existing order ack if duplicate intent arrives
        if request.order_intent_id in self._orders:
            return self._orders[request.order_intent_id]

        if random.random() < float(self.delay_probability):
            delay = float(random.uniform(0.5, 2.0))
            await asyncio.sleep(delay)

        # simulated network timeout
        if random.random() < float(self.timeout_probability):
            await asyncio.sleep(self._broker_timeout_effective_delay())
            raise asyncio.TimeoutError("simulated timeout")

        state = self._account(request.account_id)

        symbol = request.symbol
        price = request.price if request.price is not None else self._next_price(symbol)
        qty = request.quantity

        if request.side == "BUY":
            notional = qty * price
            if state.account.cash_balance < notional:
                ack = OrderAck(
                    order_id=f"ORD-{request.order_intent_id}",
                    order_intent_id=request.order_intent_id,
                    status=OrderStatus.REJECTED,
                    filled_quantity=Decimal("0"),
                    total_quantity=qty,
                    rejected_reason="INSUFFICIENT_BALANCE",
                )
                self._orders[request.order_intent_id] = ack
                return ack

            if random.random() < float(self.partial_fill_probability):
                fill_qty = (qty * Decimal(str(random.uniform(0.35, 0.85)))).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                filled_total = fill_qty
            else:
                filled_total = qty

            fill = Fill(
                order_id=f"ORD-{request.order_intent_id}",
                order_intent_id=request.order_intent_id,
                quantity=filled_total,
                price=price,
            )
            state.account.cash_balance -= filled_total * price
            position = self._positions.get((request.account_id, request.symbol))
            if position is None:
                position = Position(
                    account_id=request.account_id,
                    symbol=request.symbol,
                    quantity=filled_total,
                    average_price=price,
                )
            else:
                total_qty = position.quantity + filled_total
                weighted = (position.average_price * position.quantity) + (price * filled_total)
                position.quantity = total_qty
                position.average_price = (weighted / total_qty).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            self._positions[(request.account_id, request.symbol)] = position

            status = OrderStatus.PARTIALLY_FILLED if filled_total < qty else OrderStatus.FILLED
            ack = OrderAck(
                order_id=f"ORD-{request.order_intent_id}",
                order_intent_id=request.order_intent_id,
                status=status,
                filled_quantity=filled_total,
                total_quantity=qty,
                average_fill_price=price,
                fills=[fill],
            )
            self._orders[request.order_intent_id] = ack
            return ack

        if request.side == "SELL":
            position = self._positions.get((request.account_id, request.symbol))
            if position is None or position.quantity < qty:
                ack = OrderAck(
                    order_id=f"ORD-{request.order_intent_id}",
                    order_intent_id=request.order_intent_id,
                    status=OrderStatus.REJECTED,
                    filled_quantity=Decimal("0"),
                    total_quantity=qty,
                    rejected_reason="INSUFFICIENT_POSITION",
                )
                self._orders[request.order_intent_id] = ack
                return ack

            if random.random() < float(self.partial_fill_probability):
                fill_qty = (qty * Decimal(str(random.uniform(0.35, 0.85)))).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                filled_total = fill_qty
            else:
                filled_total = qty

            fill = Fill(
                order_id=f"ORD-{request.order_intent_id}",
                order_intent_id=request.order_intent_id,
                quantity=filled_total,
                price=price,
            )
            position.quantity -= filled_total
            if position.quantity <= Decimal("0"):
                self._positions.pop((request.account_id, request.symbol), None)

            state.account.cash_balance += filled_total * price
            status = OrderStatus.PARTIALLY_FILLED if filled_total < qty else OrderStatus.FILLED
            ack = OrderAck(
                order_id=f"ORD-{request.order_intent_id}",
                order_intent_id=request.order_intent_id,
                status=status,
                filled_quantity=filled_total,
                total_quantity=qty,
                average_fill_price=price,
                fills=[fill],
            )
            self._orders[request.order_intent_id] = ack
            return ack

        raise ValueError(f"unsupported side {request.side}")

    async def get_order_status(self, order_intent_id: str) -> OrderAck | None:
        return self._orders.get(order_intent_id)

    async def get_cash_snapshot(self, account_id: str) -> CashSnapshot:
        account = self._account(account_id)
        return CashSnapshot(
            account_id=account_id,
            cash_balance=account.account.cash_balance,
            available_cash=account.account.cash_balance,
        )

    async def get_market_tick(self, symbol: str) -> Decimal:
        if symbol not in self._prices:
            self._prices[symbol] = Decimal("10000")
        return self._next_price(symbol)

    def _broker_timeout_effective_delay(self) -> float:
        # emulate network stall within wait_for timeout window
        return 3.0
