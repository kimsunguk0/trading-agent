"""Catalyst detector/augmenter for news candidates."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from core.events.schemas import NewsEvent

from .news_analyst import NewsAnalystOutput


@dataclass
class CatalystEvent:
    symbol: str
    catalyst_score: Decimal
    breakout_confirmed: bool
    breakout_signal: Decimal
    signal_quality: str
    payload: dict[str, object]


class CatalystHunter:
    def __init__(self, min_score: Decimal = Decimal("0.72")) -> None:
        self.min_score = min_score

    async def analyze(
        self,
        event: NewsEvent,
        analysis: NewsAnalystOutput,
        *,
        latest_bars: list[tuple[str, str]] | None = None,
    ) -> CatalystEvent:
        score = analysis.catalyst_score
        symbol = str((analysis.symbol_candidates[0].code if analysis.symbol_candidates else ""))
        breakout_signal = Decimal("0")
        if latest_bars:
            # Simple breakout heuristic from recent high/low tags.
            # latest_bars item format: (high, low)
            values = [abs(Decimal(str(h)) - Decimal(str(l))) for h, l in latest_bars]
            if values:
                breakout_signal = sum(values) / Decimal(len(values))
        qualifies = score >= self.min_score
        quality = "strong" if qualifies and breakout_signal > Decimal("0") else "normal"
        return CatalystEvent(
            symbol=symbol,
            catalyst_score=score,
            breakout_confirmed=qualifies,
            breakout_signal=breakout_signal,
            signal_quality=quality,
            payload={
                "news_event": {
                    "title": event.title,
                    "source": event.source,
                    "time": event.occurred_at.isoformat() if event.occurred_at else None,
                },
                "analysis": analysis.model_dump(mode="json"),
                "min_score": str(self.min_score),
            },
        )
