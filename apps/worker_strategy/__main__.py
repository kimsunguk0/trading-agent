"""Decision strategy worker: market ticks + news context to order intents."""

from __future__ import annotations

import asyncio
import json
import os
from decimal import Decimal
from typing import Any

import redis.asyncio as redis
from pydantic import TypeAdapter

from agents.context.liquidity import load_liquidity_snapshot
from agents.context.market_regime import MarketRegimeWorker
from agents.decision.decision_engine import DecisionEngine
from agents.decision.strategy_engine import StrategyEngine
from brokers.kiwoom_rest_kr_mock import KiwoomRestKrMockAdapter
from brokers.simulated import SimulatedBrokerAdapter
from core.events.bus import RedisStreamBus
from core.events.schemas import EventType, MarketTickEvent


def _normalize_payload(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _normalize_payload(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_payload(item) for item in value]
    return value


async def _ensure_consumer_group(client: redis.Redis, stream: str, group: str) -> None:
    try:
        await client.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception as exc:  # pragma: no cover
        msg = str(exc)
        if "BUSYGROUP" not in msg and "already exists" not in msg:
            raise


def _select_broker() -> object:
    adapter_name = os.getenv("BROKER_ADAPTER", "simulated").lower()
    if adapter_name == "kiwoom_mock":
        return KiwoomRestKrMockAdapter()
    return SimulatedBrokerAdapter()


class StrategyWorker:
    def __init__(self) -> None:
        self.environment = os.getenv("ENVIRONMENT", "paper").lower()
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.stream_prefix = os.getenv("REDIS_STREAM_PREFIX", f"{self.environment}.events")
        self.account_id = os.getenv("ACCOUNT_ID", "default")
        self.schema = f"trading_{self.environment}"
        self.dsn = os.getenv("DATABASE_URL")

        self.stream_signals = f"{self.stream_prefix}.{EventType.SIGNAL.value}"
        self.stream_ticks = f"{self.stream_prefix}.{EventType.MARKET_TICK.value}"
        self.stream_regime = f"{self.stream_prefix}.regime"

        self.strategy_engine = StrategyEngine(
            account_id=self.account_id,
            redis_url=self.redis_url,
            stream_prefix=self.stream_prefix,
        )

        self._bus = RedisStreamBus(redis_url=self.redis_url, stream_prefix=self.stream_prefix)
        raw_client = redis.from_url(self.redis_url, decode_responses=True)
        self._client = raw_client
        broker = _select_broker()
        self.decision_engine = DecisionEngine(
            broker=broker,
            bus=self._bus,
            account_id=self.account_id,
        )

        strategy_symbols: list[str] = []
        for strategy in self.strategy_engine.news_breakout_configs:
            strategy_symbols.extend([str(item) for item in strategy.symbols])
        self.regime_symbols = strategy_symbols or ["005930"]
        self.regime_worker = MarketRegimeWorker(
            environment=self.environment,
            redis_url=self.redis_url,
            symbols=self.regime_symbols,
            market="KR",
            schema=self.schema,
            run_interval_seconds=300,
        )

        self.signal_group = os.getenv("WORKER_STRATEGY_SIGNAL_GROUP", "worker_strategy_signals")
        self.signal_consumer = os.getenv("WORKER_STRATEGY_SIGNAL_CONSUMER", "worker_strategy_signals_0")
        self.tick_group = os.getenv("WORKER_STRATEGY_TICK_GROUP", "worker_strategy_ticks")
        self.tick_consumer = os.getenv("WORKER_STRATEGY_TICK_CONSUMER", "worker_strategy_ticks_0")

    def _build_candidate_message(self, signal: Any) -> dict[str, Any]:
        payload = signal.payload if isinstance(signal.payload, dict) else {}
        risk = payload.get("risk", {}) if isinstance(payload.get("risk", {}), dict) else {}
        execution = payload.get("execution", {}) if isinstance(payload.get("execution", {}), dict) else {}
        return {
            "event_type": "news_candidate",
            "symbol": payload.get("code", str(signal.symbol)),
            "code": payload.get("code", str(signal.symbol)),
            "strategy_id": payload.get("strategy_id", str(signal.strategy_id)),
            "confidence": payload.get("confidence", "0"),
            "regime": payload.get("regime", ""),
            "news_summary": payload.get("news_summary", ""),
            "technical_summary": payload.get("technical_summary", ""),
            "risk": risk,
            "execution": execution,
            "price": payload.get("price", "0"),
            "quantity": payload.get("quantity", "1"),
            "symbol_name": payload.get("symbol_name", ""),
        }

    async def _refresh_market_context(self) -> None:
        while True:
            regime = await self.regime_worker.run_once()
            self.strategy_engine.update_regime(regime)

            for strategy in self.strategy_engine.news_breakout_configs:
                symbols = [str(item) for item in strategy.symbols]
                if not symbols:
                    continue

                threshold = strategy.liquidity_min_avg_value_20d_krw
                for symbol in symbols:
                    self.strategy_engine.set_liquidity_snapshot(strategy.strategy_id, symbol, Decimal("0"))

                if threshold <= Decimal("0"):
                    continue

                snapshot = await load_liquidity_snapshot(
                    symbols=symbols,
                    schema=self.schema,
                    market=strategy.market,
                    min_avg_value_20d_krw=threshold,
                    dsn=self.dsn,
                )
                for symbol, avg_value in snapshot.values.items():
                    self.strategy_engine.set_liquidity_snapshot(strategy.strategy_id, symbol, avg_value)

            await asyncio.sleep(300)

    async def _consume_news_analysis(self) -> None:
        while True:
            entries = await self._client.xreadgroup(
                self.signal_group,
                self.signal_consumer,
                {self.stream_signals: ">"},
                count=16,
                block=2000,
            )
            if not entries:
                continue

            for _stream, messages in entries:
                for _message_id, fields in messages:
                    raw = fields.get("payload") if isinstance(fields, dict) else None
                    if not isinstance(raw, str):
                        continue
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue

                    # Ignore anything except worker_agent output
                    if payload.get("event_type") != "news_analysis":
                        continue
                    self.strategy_engine.set_news_payload(payload)

    async def _consume_market_ticks(self) -> None:
        adapter = TypeAdapter(MarketTickEvent)
        while True:
            entries = await self._client.xreadgroup(
                self.tick_group,
                self.tick_consumer,
                {self.stream_ticks: ">"},
                count=32,
                block=2000,
            )
            if not entries:
                continue

            for _stream, messages in entries:
                for _message_id, fields in messages:
                    raw = fields.get("payload") if isinstance(fields, dict) else None
                    if not isinstance(raw, str):
                        continue
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        continue
                    try:
                        tick = adapter.validate_python(payload)
                    except Exception:
                        continue

                    for signal in self.strategy_engine.evaluate(tick):
                        order_intent = await self.decision_engine.process_signal(signal)
                        if order_intent is None:
                            continue

                        candidate = self._build_candidate_message(signal)
                        candidate["order_intent_id"] = order_intent.request.order_intent_id
                        candidate["status"] = "PAPER 자동 발주 예정"
                        await self._client.xadd(
                            self.stream_signals,
                            {"payload": json.dumps(_normalize_payload(candidate), ensure_ascii=False)},
                        )

    async def run(self) -> None:
        await _ensure_consumer_group(self._client, self.stream_signals, self.signal_group)
        await _ensure_consumer_group(self._client, self.stream_ticks, self.tick_group)

        await asyncio.gather(
            self._consume_news_analysis(),
            self._consume_market_ticks(),
            self._refresh_market_context(),
        )

    async def close(self) -> None:
        await self._client.aclose()


async def main() -> None:
    worker = StrategyWorker()
    try:
        await worker.run()
    finally:
        await worker.close()


if __name__ == "__main__":
    asyncio.run(main())
