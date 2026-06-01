"""News collector for MVP2.

Polls RSS sources and writes items to Redis/news table.

- KR: NAVER/Hankyung/Yonhap
- US: Bloomberg/Reuters
- US earnings disclosures: SEC EDGAR XBRL API
- language detection via langdetect (ko / en distinction)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from xml.etree import ElementTree

import asyncpg
import aiohttp

from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException

from core.events.bus import RedisStreamBus
from core.events.schemas import EventType, NewsEvent


NAVER_FINANCE_RSS = "https://finance.naver.com/rss"
HANKYUNG_RSS = "https://www.hankyung.com/feed"
YONHAP_INFORMAX_RSS = "https://www.yna.co.kr/rss"
BLOOMBERG_RSS = "https://www.bloomberg.com/feeds/politics/latest.xml"
REUTERS_RSS = "https://www.reuters.com/arc/outboundfeeds/rss/?outputType=xml"
EDGAR_BASE_URL = "https://data.sec.gov/api/xbrl"


@dataclass
class RawNewsItem:
    source: str
    title: str
    body: str
    published_at: datetime
    source_url: str
    language: str = "unknown"


def _to_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        pass
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%d",
        "%Y%m%d",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return datetime.now(timezone.utc)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("\n", " ").replace("\r", " ")
    return text.strip()


def _body_hash(source: str, title: str, body: str) -> str:
    digest = hashlib.sha256()
    digest.update(source.encode("utf-8"))
    digest.update(b"|")
    digest.update(_normalize_text(title).encode("utf-8"))
    digest.update(b"|")
    digest.update(_normalize_text(body).encode("utf-8"))
    return digest.hexdigest()


def _coerce_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _detect_language(text: str) -> str:
    if not text:
        return "unknown"
    try:
        lang = detect(text)
    except LangDetectException:
        return "unknown"
    if lang == "ko":
        return "KR"
    if lang.startswith("en"):
        return "EN"
    return lang.upper()


def _coerce_json(raw: str) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return {}


def _normalize_fetch_headers() -> dict[str, str]:
    headers = {"user-agent": "Mozilla/5.0 (compatible; trading-bot/1.0)"}
    sec_user_agent = os.getenv("SEC_USER_AGENT")
    if sec_user_agent:
        headers["user-agent"] = sec_user_agent
    return headers


def _edgar_symbols() -> list[str]:
    raw = os.getenv("EDGAR_SYMBOLS", "").strip()
    if raw:
        return [item.strip().upper() for item in raw.split(",") if item.strip()]
    return []


def _edgar_cik_map() -> dict[str, str]:
    raw = os.getenv("EDGAR_CIK_MAP", "").strip()
    if not raw:
        return {}
    parsed = _coerce_json(raw)
    if not isinstance(parsed, dict):
        return {}
    out: dict[str, str] = {}
    for symbol, cik in parsed.items():
        if symbol and cik:
            out[str(symbol).strip().upper()] = str(cik).strip().zfill(10)
    return out


def _edgar_url_for_symbol(symbol: str, cik: str) -> str:
    # Companyfacts endpoint gives XBRL tags, including US GAAP earnings/revenue facts.
    return f"{EDGAR_BASE_URL}/companyfacts/CIK{cik}.json"


def _to_edgar_numeric(value: Any) -> Decimal:
    try:
        if value is None:
            return Decimal("0")
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


class NewsCollector:
    def __init__(
        self,
        environment: str = "paper",
        redis_url: str = "redis://localhost:6379/0",
        redis_prefix: str = "paper.events",
        poll_interval_seconds: int = 300,
        dsn: str | None = None,
    ) -> None:
        self.environment = environment
        self.redis_url = redis_url
        self.redis_prefix = redis_prefix
        self.poll_interval_seconds = poll_interval_seconds
        self.bus = RedisStreamBus(redis_url=redis_url, stream_prefix=redis_prefix)
        self.schema = f"trading_{environment}"
        self.dsn = dsn or os.getenv("DATABASE_URL")
        self._seen_hashes: set[str] = set()

    async def _fetch(self, session: aiohttp.ClientSession, url: str, *, params: dict[str, str] | None = None) -> str:
        async with session.get(url, params=params, timeout=20, headers=_normalize_fetch_headers()) as response:
            response.raise_for_status()
            return await response.text(encoding="utf-8", errors="ignore")

    def _parse_rss(self, source: str, raw: str) -> list[RawNewsItem]:
        root = ElementTree.fromstring(raw)
        items: list[RawNewsItem] = []

        for node in root.findall(".//item"):
            title = _normalize_text(node.findtext("title"))
            link = _normalize_text(node.findtext("link"))
            desc = _normalize_text(node.findtext("description"))
            body = desc or title
            if not title and not body:
                continue
            pub = _to_datetime(_normalize_text(node.findtext("pubDate")))
            language = _detect_language(f"{title} {body}")
            items.append(RawNewsItem(source=source, title=title, body=body, published_at=pub, source_url=link, language=language))
        return items

    def _extract_edgar_fact(self, facts: dict[str, Any], tag: str) -> tuple[Decimal, Decimal, Decimal]:
        # returns: value_actual, value_estimate, surprise_pct
        if not isinstance(facts, dict):
            return (Decimal("0"), Decimal("0"), Decimal("0"))

        frame = facts.get(tag)
        if not isinstance(frame, dict):
            return (Decimal("0"), Decimal("0"), Decimal("0"))

        units = frame.get("units")
        if not isinstance(units, dict):
            return (Decimal("0"), Decimal("0"), Decimal("0"))

        values = []
        for unit in units.values():
            if not isinstance(unit, list):
                continue
            for row in unit:
                if not isinstance(row, dict):
                    continue
                dt = str(row.get("fp", "")).upper()
                if dt not in {"FY", "Q1", "Q2", "Q3", "Q4", "TTM"}:
                    continue
                actual = _to_edgar_numeric(row.get("val"))
                period = _to_edgar_numeric(row.get("end"))
                values.append((actual, str(row.get("form", "")), period))

        if not values:
            return (Decimal("0"), Decimal("0"), Decimal("0"))

        latest = values[0][0]
        estimate = max(values[0][2], Decimal("1")) if len(values) > 1 else Decimal("0")
        surprise = Decimal("0")
        if estimate:
            surprise = ((latest - estimate) / estimate) * Decimal("100")
        return latest, estimate, surprise

    async def _collect_rss(self, session: aiohttp.ClientSession, source: str, url: str) -> list[RawNewsItem]:
        try:
            raw = await self._fetch(session, url)
            return self._parse_rss(source, raw)
        except Exception:
            return []

    async def _collect_dart(self, session: aiohttp.ClientSession) -> list[RawNewsItem]:
        api_key = os.getenv("DART_API_KEY")
        if not api_key:
            return []

        items: list[RawNewsItem] = []
        endpoint = f"https://opendart.fss.or.kr/api/list.json"
        today = datetime.now(timezone.utc)
        end = today.strftime("%Y%m%d")
        start = (today.replace(hour=0, minute=0, second=0, microsecond=0)).strftime("%Y%m%d")
        params = {
            "crtfc_key": api_key,
            "bgn_de": start,
            "end_de": end,
            "page_no": "1",
            "page_count": "100",
        }
        try:
            raw = await self._fetch(session, endpoint, params=params)
            payload = _coerce_json(raw)
            for row in payload.get("list", []) if isinstance(payload, dict) else []:
                if not isinstance(row, dict):
                    continue
                title = _normalize_text(row.get("report_nm") or row.get("corp_name") or row.get("rpt_nm"))
                body = _normalize_text(row.get("summary") or title)
                source_url = _normalize_text(row.get("rcept_no"))
                published_at = _to_datetime(str(row.get("rcept_dt")))
                items.append(
                    RawNewsItem(
                        source="DART",
                        title=title,
                        body=body,
                        published_at=published_at,
                        source_url=f"{source_url}",
                        language="KR",
                    )
                )
        except Exception:
            return []
        return items

    async def _collect_edgar(self, session: aiohttp.ClientSession) -> list[RawNewsItem]:
        symbols = _edgar_symbols()
        if not symbols:
            return []
        cik_map = _edgar_cik_map()
        if not cik_map:
            return []

        items: list[RawNewsItem] = []
        for symbol in symbols:
            cik = cik_map.get(symbol)
            if not cik:
                continue
            try:
                endpoint = _edgar_url_for_symbol(symbol, cik)
                raw = await self._fetch(session, endpoint)
                payload = _coerce_json(raw)
                facts = (
                    (payload.get("facts", {}) or {})
                    .get("us-gaap", {})
                    if isinstance(payload, dict)
                    else {}
                )
                eps_actual, eps_est, eps_surprise = self._extract_edgar_fact(facts.get("EarningsPerShareBasic", {}) if isinstance(facts, dict) else {}, "EarningsPerShareBasic")
                rev_actual, rev_est, rev_surprise = self._extract_edgar_fact(facts.get("SalesRevenueNet", {}) if isinstance(facts, dict) else {}, "SalesRevenueNet")

                title = f"EDGAR XBRL earnings update: {symbol}"
                body = (
                    f"Symbol={symbol} "
                    f"EPS={eps_actual} (estimate {eps_est}, surprise {eps_surprise}%) "
                    f"Revenue={rev_actual} (estimate {rev_est}, surprise {rev_surprise}%)"
                )
                items.append(
                    RawNewsItem(
                        source="EDGAR",
                        title=title,
                        body=body,
                        published_at=_to_datetime(None),
                        source_url=f"https://www.sec.gov/ixviewer/action?meta={symbol}",
                        language="EN",
                    )
                )
            except Exception:
                continue
        return items

    async def collect_once(self) -> list[RawNewsItem]:
        async with aiohttp.ClientSession() as session:
            tasks = [
                self._collect_rss(session, "NAVER_FINANCE", NAVER_FINANCE_RSS),
                self._collect_rss(session, "HANKYUNG", HANKYUNG_RSS),
                self._collect_rss(session, "YONHAP_INFORMAX", YONHAP_INFORMAX_RSS),
                self._collect_rss(session, "BLOOMBERG_RSS", BLOOMBERG_RSS),
                self._collect_rss(session, "REUTERS_RSS", REUTERS_RSS),
                self._collect_dart(session),
                self._collect_edgar(session),
            ]
            nested = await asyncio.gather(*tasks)

        merged: list[RawNewsItem] = []
        for payload in nested:
            merged.extend(payload)
        return merged

    async def _insert_news_item(self, conn: asyncpg.Connection, row: RawNewsItem, content_hash: str) -> None:
        await conn.execute(
            f"""
            INSERT INTO {self.schema}.news_items (source, url, title, body, body_hash, published_at, raw_json, language)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (url) DO NOTHING
            """,
            row.source,
            row.source_url,
            row.title,
            row.body,
            content_hash,
            row.published_at,
            {
                "source": row.source,
                "title": row.title,
                "body": row.body,
                "language": row.language,
                "published_at": row.published_at.isoformat(),
                "source_url": row.source_url,
            },
            row.language,
        )

    async def _persist_and_publish(self, items: list[RawNewsItem]) -> None:
        conn: asyncpg.Connection | None = None
        if self.dsn:
            try:
                conn = await asyncpg.connect(self.dsn)
            except Exception:
                conn = None

        try:
            for item in items:
                content_hash = _body_hash(item.source, item.title, item.body)
                if content_hash in self._seen_hashes:
                    continue

                if conn is not None:
                    try:
                        await self._insert_news_item(conn, item, content_hash)
                    except Exception:
                        # DB side constraint or transient error should not block stream ingestion.
                        pass

                event = NewsEvent(
                    event_type=EventType.NEWS,
                    title=item.title,
                    body=item.body,
                    source=item.source,
                    occurred_at=item.published_at,
                    payload={
                        "body_hash": content_hash,
                        "source_url": item.source_url,
                        "language": item.language,
                        "body": item.body,
                    },
                )
                await self.bus.publish(event)
                self._seen_hashes.add(content_hash)
        finally:
            if conn is not None:
                await conn.close()

    async def run_once(self) -> int:
        items = await self.collect_once()
        if not items:
            return 0
        await self._persist_and_publish(items)
        return len(items)

    async def run(self) -> None:
        while True:
            await self.run_once()
            await asyncio.sleep(max(30, int(self.poll_interval_seconds)))


async def run() -> None:
    env = os.getenv("ENVIRONMENT", "paper")
    collector = NewsCollector(
        environment=env,
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        redis_prefix=os.getenv("REDIS_STREAM_PREFIX", f"{env}.events"),
        poll_interval_seconds=int(os.getenv("NEWS_POLL_INTERVAL_SECONDS", "300")),
    )
    await collector.run()


if __name__ == "__main__":
    import asyncio

    asyncio.run(run())
