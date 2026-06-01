"""Corporate action collector for DART OpenAPI events."""

from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import asyncpg
import httpx
import yaml

from core.events.bus import RedisStreamBus
from core.events.schemas import CorporateActionEvent, EventType


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            value = row[key]
            if value != "":
                return value
    return None


def _first_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coalesce_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    patterns = ["%Y%m%d", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y%m%d%H%M%S"]
    for fmt in patterns:
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


@dataclass
class CorporateActionRecord:
    market: str
    symbol: str
    action_type: str
    title: str | None
    as_of: datetime
    cash_amount: Decimal | None
    shares_per_stock: Decimal | None
    ratio: Decimal | None
    raw: dict[str, Any]


class CorporateActionCollector:
    DART_BASE = "https://opendart.fss.or.kr/api"

    def __init__(
        self,
        dart_api_key: str,
        env: str = "paper",
        redis_url: str = "redis://localhost:6379/0",
        redis_prefix: str = "paper.events",
    ) -> None:
        self.dart_api_key = dart_api_key
        self.env = env
        self.schema = f"trading_{env}"
        self.bus = RedisStreamBus(redis_url=redis_url, stream_prefix=redis_prefix)
        self._last_seen: set[str] = set()

    def _symbol(self, row: dict[str, Any]) -> str | None:
        return _first_text(
            _first(
                row,
                "stock_code",
                "srtn_cd",
                "symbol",
                "itmsNm",
                "stock",
            )
        )

    def _title(self, row: dict[str, Any]) -> str | None:
        return _first_text(_first(row, "report_nm", "rpt_nm", "title", "prps"))

    def _ratio(self, row: dict[str, Any]) -> Decimal | None:
        for key in ("ratio", "right_ratio", "rght_rt", "dvps" , "spcl_law_rgt", "myst"):
            value = row.get(key)
            if value in (None, "", 0, "0"):
                continue
            try:
                return _to_decimal(value)
            except Exception:
                continue
        return None

    def _shares_per_stock(self, row: dict[str, Any]) -> Decimal | None:
        for key in ("shares_per_stock", "stock_num", "ratio_stock", "acnt"):
            value = row.get(key)
            if value in (None, "", 0, "0"):
                continue
            try:
                return _to_decimal(value)
            except Exception:
                continue
        return None

    def _cash_amount(self, row: dict[str, Any]) -> Decimal | None:
        for key in ("cash", "cash_amount", "dvide", "dvdn", "krw"):
            value = row.get(key)
            if value in (None, "", 0, "0"):
                continue
            try:
                return _to_decimal(value)
            except Exception:
                continue
        return None

    def _parse_rows(self, rows: list[dict[str, Any]], action_type: str) -> list[CorporateActionRecord]:
        results: list[CorporateActionRecord] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = self._symbol(row)
            if not symbol:
                continue

            as_of = _coalesce_datetime(_first(row, "rcept_dt", "basDt", "report_dt", "rpt_dt", "dt"))
            if as_of is None:
                as_of = datetime.now(timezone.utc)

            title = self._title(row)
            cash_amount = self._cash_amount(row)
            shares_per_stock = self._shares_per_stock(row)
            ratio = self._ratio(row)
            record = CorporateActionRecord(
                market="KR",
                symbol=str(symbol),
                action_type=action_type,
                title=title,
                as_of=as_of,
                cash_amount=cash_amount,
                shares_per_stock=shares_per_stock,
                ratio=ratio,
                raw=row,
            )
            results.append(record)
        return results

    def _dedupe(self, record: CorporateActionRecord) -> bool:
        key = f"{record.market}:{record.symbol}:{record.action_type}:{record.as_of.isoformat()}:{record.title or ''}:{record.cash_amount}:{record.shares_per_stock}:{record.ratio}"
        if key in self._last_seen:
            return False
        self._last_seen.add(key)
        return True

    async def _request(self, endpoint: str) -> list[dict[str, Any]]:
        params = {
            "crtfc_key": self.dart_api_key,
            "page_no": "1",
            "page_count": "100",
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{self.DART_BASE}/{endpoint}", params=params)
            response.raise_for_status()
            payload = response.json()
        rows = payload.get("list")
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        return []

    async def _publish(self, record: CorporateActionRecord) -> None:
        event = CorporateActionEvent(
            event_type=EventType.CORPORATE_ACTION,
            market=record.market,
            symbol=record.symbol,
            action_type=record.action_type,
            title=record.title,
            cash_amount=record.cash_amount,
            shares_per_stock=record.shares_per_stock,
            ratio=record.ratio,
            as_of=record.as_of,
        )
        await self.bus.publish(event)

    async def _store(self, conn: asyncpg.Connection, record: CorporateActionRecord) -> None:
        await conn.execute(
            f"""
            INSERT INTO {self.schema}.corporate_actions
                (market, symbol, action_type, title, cash_amount, shares_per_stock, ratio, as_of, raw_json)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT DO NOTHING
            """,
            record.market,
            record.symbol,
            record.action_type,
            record.title,
            record.cash_amount,
            record.shares_per_stock,
            record.ratio,
            record.as_of,
            record.raw,
        )

    async def collect_once(self) -> list[CorporateActionRecord]:
        rows_div = await self._request("stockDiv.json")
        rows_mvnt = await self._request("stockMvnt.json")
        actions = self._parse_rows(rows_div, "DIVIDEND") + self._parse_rows(rows_mvnt, "BONUS")

        if not actions:
            return []

        conn = None
        if os.getenv("DATABASE_URL"):
            try:
                conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
            except Exception:
                conn = None

        published: list[CorporateActionRecord] = []
        try:
            for action in actions:
                if not self._dedupe(action):
                    continue
                await self._publish(action)
                if conn is not None:
                    await self._store(conn, action)
                published.append(action)
        finally:
            if conn is not None:
                await conn.close()

        return published


async def run() -> None:
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        return

    env = os.getenv("ENVIRONMENT", "paper").lower()
    if env not in {"paper", "live"}:
        env = "paper"

    collector = CorporateActionCollector(
        dart_api_key=api_key,
        env=env,
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        redis_prefix=os.getenv("REDIS_STREAM_PREFIX", f"{env}.events"),
    )
    interval = int(os.getenv("COLLECT_INTERVAL_SECONDS", "300"))

    while True:
        await collector.collect_once()
        await asyncio.sleep(interval)


if __name__ == "__main__":
    import asyncio

    asyncio.run(run())
