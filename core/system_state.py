"""System-wide state machine with transition logging and deterministic semantics."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

try:
    import asyncpg
except Exception:  # pragma: no cover
    asyncpg = None

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None


class SystemState(str, Enum):
    NORMAL = "NORMAL"
    DEGRADED_NEWS = "DEGRADED_NEWS"
    DEGRADED_LLM = "DEGRADED_LLM"
    DEGRADED_MARKET = "DEGRADED_MARKET"
    BROWNOUT = "BROWNOUT"
    HALTED = "HALTED"
    EMERGENCY_STOP = "EMERGENCY_STOP"

    # Backward compatibility
    DEGRADED_DATA = DEGRADED_NEWS
    DEGRADED_BROKER = DEGRADED_MARKET


@dataclass
class SystemStateEvent:
    from_state: SystemState
    to_state: SystemState
    reason: str
    triggered_by: str
    timestamp: datetime


class SystemStateManager:
    """Centralized state machine used by monitors, bots, and workers."""

    def __init__(
        self,
        initial: SystemState = SystemState.NORMAL,
        *,
        environment: str = "paper",
        redis_url: str | None = None,
        stream_prefix: str | None = None,
        dsn: str | None = None,
    ) -> None:
        self.state = initial
        self.environment = environment
        self.redis_url = redis_url
        self.stream_prefix = stream_prefix or f"{environment}.events"
        self.dsn = dsn

        self._manual_override: SystemState | None = None
        self._component_degraded: dict[str, bool] = {
            "news": False,
            "llm": False,
            "market_data": False,
            "ws_heartbeat": False,
            "broker_response": False,
            "balance_check": False,
            "broker_api": False,
            "other": False,
        }
        self._component_to_state: dict[str, SystemState] = {
            "news": SystemState.DEGRADED_NEWS,
            "llm": SystemState.DEGRADED_LLM,
            "market_data": SystemState.DEGRADED_MARKET,
            "ws_heartbeat": SystemState.DEGRADED_MARKET,
            "broker_response": SystemState.DEGRADED_MARKET,
            "balance_check": SystemState.DEGRADED_MARKET,
            "broker_api": SystemState.BROWNOUT,
            "other": SystemState.DEGRADED_MARKET,
        }
        self._last_event = SystemStateEvent(
            from_state=initial,
            to_state=initial,
            reason="boot",
            triggered_by="system",
            timestamp=datetime.now(timezone.utc),
        )

    @property
    def last_event(self) -> SystemStateEvent:
        return self._last_event

    @property
    def is_halted(self) -> bool:
        return self.state in {SystemState.HALTED, SystemState.EMERGENCY_STOP}

    def allow_news_signals(self) -> bool:
        return self.state not in {
            SystemState.DEGRADED_NEWS,
            SystemState.BROWNOUT,
            SystemState.HALTED,
            SystemState.EMERGENCY_STOP,
        }

    def allow_llm_calls(self) -> bool:
        return self.state not in {
            SystemState.DEGRADED_LLM,
            SystemState.BROWNOUT,
            SystemState.HALTED,
            SystemState.EMERGENCY_STOP,
        }

    def allow_polling_fallback(self) -> bool:
        return self.state != SystemState.EMERGENCY_STOP

    def allow_new_market_entries(self) -> bool:
        return self.state in {
            SystemState.NORMAL,
            SystemState.DEGRADED_NEWS,
            SystemState.DEGRADED_LLM,
        }

    def allow_new_orders(self) -> bool:
        return self.state not in {
            SystemState.HALTED,
            SystemState.EMERGENCY_STOP,
        }

    def allow_new_entries(self) -> bool:
        return self.allow_new_market_entries()

    def allow_stop_loss_updates(self) -> bool:
        return self.state in {
            SystemState.NORMAL,
            SystemState.DEGRADED_NEWS,
            SystemState.DEGRADED_LLM,
            SystemState.DEGRADED_MARKET,
            SystemState.BROWNOUT,
        }

    def allow_open_order_cancellation(self) -> bool:
        return self.state == SystemState.HALTED

    @property
    def is_healthy(self) -> bool:
        return self.state == SystemState.NORMAL

    def _allow(self, *states: SystemState) -> bool:
        return self.state in set(states)

    @staticmethod
    def _is_human_actor(actor: str) -> bool:
        return str(actor).strip().lower() in {
            "user",
            "human",
            "operator",
            "trader",
            "admin",
            "telegram",
            "resume_paper",
            "resume",
            "mode",
            "paper",
        }

    async def _publish_state_event_async(self, state: SystemState, reason: str, triggered_by: str) -> None:
        if redis is None or not self.redis_url:
            return

        client = redis.from_url(self.redis_url, decode_responses=True)
        try:
            payload = {
                "event_type": "system_state",
                "environment": self.environment,
                "state": state.value,
                "reason": reason,
                "triggered_by": triggered_by,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
            }
            await client.xadd(f"{self.stream_prefix}.system_state", {"payload": json.dumps(payload, ensure_ascii=False)})
        except Exception:
            return
        finally:
            closer = getattr(client, "aclose", None) or getattr(client, "close", None)
            if closer is not None:
                result = closer()
                if hasattr(result, "__await__"):
                    await result

    def _publish_state_event(self, state: SystemState, reason: str, triggered_by: str) -> None:
        self._schedule_async(self._publish_state_event_async(state, reason, triggered_by))

    def _schedule_async(self, coro: Any) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            closer = getattr(coro, "close", None)
            if callable(closer):
                closer()
            return
        loop.create_task(coro)

    def _schema(self) -> str:
        return f"trading_{self.environment}"

    async def _log_transition(
        self,
        from_state: SystemState,
        to_state: SystemState,
        reason: str,
        triggered_by: str,
    ) -> None:
        if asyncpg is None or not self.dsn:
            return

        conn = await asyncpg.connect(self.dsn)
        try:
            try:
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema()}.system_state_log (
                        from_state,
                        to_state,
                        reason,
                        triggered_by
                    ) VALUES ($1, $2, $3, $4)
                    """,
                    from_state.value,
                    to_state.value,
                    reason,
                    triggered_by,
                )
            except Exception:
                await conn.execute(
                    f"""
                    INSERT INTO {self._schema()}.system_state_log (state, reason)
                    VALUES ($1, $2)
                    """,
                    to_state.value,
                    f"[{from_state.value}] {reason}",
                )
        finally:
            await conn.close()

    def _transition(self, next_state: SystemState, reason: str, *, triggered_by: str) -> SystemState:
        if next_state == self.state:
            return self.state

        previous = self.state
        self.state = next_state
        self._last_event = SystemStateEvent(
            from_state=previous,
            to_state=next_state,
            reason=reason,
            triggered_by=triggered_by,
            timestamp=datetime.now(timezone.utc),
        )
        self._publish_state_event(next_state, reason, triggered_by)
        self._schedule_async(self._log_transition(previous, next_state, reason, triggered_by))
        return next_state

    def transition_to(self, next_state: SystemState, reason: str, *, actor: str = "system") -> SystemState:
        if next_state == self.state:
            return self.state

        if (
            self.state == SystemState.EMERGENCY_STOP
            and next_state != SystemState.EMERGENCY_STOP
            and not self._is_human_actor(actor)
        ):
            return self.state

        if self.state == SystemState.HALTED and next_state != SystemState.HALTED and not self._is_human_actor(actor):
            return self.state

        return self._transition(next_state, reason, triggered_by=actor)

    def _active_degraded_count(self) -> int:
        return sum(1 for value in self._component_degraded.values() if value)

    def _highest_priority_degraded(self) -> SystemState | None:
        for component, is_degraded in self._component_degraded.items():
            if is_degraded:
                mapped = self._component_to_state.get(component)
                if mapped is None:
                    return SystemState.DEGRADED_MARKET
                return mapped
        return None

    def _target_from_components(self) -> SystemState | None:
        if self._active_degraded_count() >= 2:
            return SystemState.BROWNOUT
        target = self._highest_priority_degraded()
        if target is None:
            return None
        return target

    def set_component_state(
        self,
        component: str,
        degraded: bool,
        reason: str,
        *,
        triggered_by: str = "system",
        auto_recover: bool = True,
    ) -> SystemState:
        if self.state in {SystemState.HALTED, SystemState.EMERGENCY_STOP} and auto_recover:
            return self.state

        component_key = str(component).strip().lower()
        if component_key not in self._component_degraded:
            component_key = "other"

        self._component_degraded[component_key] = bool(degraded)

        target = self._target_from_components()
        if target is None:
            if self._manual_override in {SystemState.HALTED, SystemState.EMERGENCY_STOP}:
                return self.state

            if self.state in {
                SystemState.DEGRADED_NEWS,
                SystemState.DEGRADED_LLM,
                SystemState.DEGRADED_MARKET,
                SystemState.BROWNOUT,
            }:
                return self._transition(SystemState.NORMAL, "all_degraded_cleared", triggered_by=triggered_by)
            return self.state

        # In brownout, we intentionally enforce position-only semantics.
        return self._transition(target, reason, triggered_by=triggered_by)

    def set_state(self, next_state: SystemState, reason: str, *, triggered_by: str = "system") -> SystemState:
        self._manual_override = None
        if next_state == self.state:
            return self.state

        self._component_degraded = {
            "news": False,
            "llm": False,
            "market_data": False,
            "ws_heartbeat": False,
            "broker_response": False,
            "balance_check": False,
            "broker_api": False,
            "other": False,
        }
        return self.transition_to(next_state, reason, actor=triggered_by)

    def halt(self, reason: str = "user_halt", *, actor: str = "user") -> SystemState:
        self._manual_override = SystemState.HALTED
        return self._transition(SystemState.HALTED, reason, triggered_by=actor)

    def emergency_stop(self, reason: str = "system_critical", *, actor: str = "system") -> SystemState:
        self._manual_override = SystemState.EMERGENCY_STOP
        return self._transition(SystemState.EMERGENCY_STOP, reason, triggered_by=actor)

    def cancel_open_orders_only(self, *, reason: str = "halting", actor: str = "user") -> None:
        self._manual_override = SystemState.HALTED
        self._transition(SystemState.HALTED, reason, triggered_by=actor)

    def resume(self, reason: str = "resume", *, actor: str = "user", source: str | None = None) -> SystemState:
        delimited_source = source or actor
        if self.state == SystemState.EMERGENCY_STOP and not self._is_human_actor(actor):
            return self.state
        if self._manual_override not in {SystemState.HALTED, SystemState.EMERGENCY_STOP}:
            return self.state

        self._manual_override = None
        if self.state in {
            SystemState.EMERGENCY_STOP,
            SystemState.HALTED,
        }:
            target = self._target_from_components()
            return self._transition(target or SystemState.NORMAL, reason, triggered_by=delimited_source)

        return self.state

    def human_resume(self, reason: str = "resume", *, actor: str = "user", source: str | None = None) -> SystemState:
        return self.resume(reason=reason, actor=actor, source=source)

    def can_resume_emergency(self, reason: str = "resume", *, actor: str = "user", source: str | None = None) -> SystemState:
        return self.resume(reason=reason, actor=actor, source=source)

    @property
    def can_resume_command(self) -> tuple[bool, str]:
        allowed = self.state in {SystemState.HALTED, SystemState.EMERGENCY_STOP} and self._manual_override in {
            SystemState.HALTED,
            SystemState.EMERGENCY_STOP,
        }
        return allowed, f"state={self.state.value}"

    def __repr__(self) -> str:
        return f"SystemStateManager(state={self.state}, manual={self._manual_override}, dsn={bool(self.dsn)})"


# Backward compatibility.
SystemStateMachine = SystemStateManager
