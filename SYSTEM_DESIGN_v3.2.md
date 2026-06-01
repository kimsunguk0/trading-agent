# 주식 자동매매 멀티 에이전트 시스템 — 전체 설계서 (v3.2)

> **버전**: v3.2 (2026-05-29)
> **목표**: 한국·미국 주식을 24시간 자동으로 매매하는 멀티 에이전트 시스템.
> 현재 키움 REST 모의투자로 검증하고, 토스증권 API 출시 시 어댑터만 교체.
> ChatOps(Telegram·Slack·Discord)를 관제센터로 사용.

## 변경 이력

### v3.2 (2026-05-29) — 컨테이너 운영 전략 보강
§15 운영 인프라 대폭 확장 (40줄 → 220줄):
- **§15.2 컨테이너 5계층 그룹화**: Infra / GPU / Data / Decision / Critical. 그룹별 의존성·재시작 정책·헬스체크 명시.
- **§15.3 MVP 단계별 컨테이너**: 8 → 9 → 14 → 18개로 점진적 증가. 어느 단계에 무엇을 추가하는지 명확화.
- **§15.4 분리·합침 규칙**: 절대 합치면 안 되는 7개 조합 + 분리의 3가지 신호.
- **§15.5 자원 할당 표**: 컨테이너별 RAM/GPU/CPU 권장값 + GPU 공유 전략.
- **§15.6 Paper/Live Docker Compose 분리**: §9.1.1과 일관성 있게 compose 파일 분리.
- **§15.7 헬스체크 & 재시작 정책**: 그룹별 정책, Critical 그룹은 자체 watchdog.
- Critical 그룹 특별 규칙: **infra에만 의존, 다른 모든 게 죽어도 살아있어야 함**.

### v3.1 (2026-05-27) — 안전성·정합성 패치
9개 항목 패치:
1. **LLM 권한 명확화**: LLM은 시그널·랭킹·설명까지만. `order_intent` 생성은 결정론적 `DecisionPolicyEngine` 전담.
2. **SimulatedBrokerAdapter 우선**: MVP 0에서 외부 API 없이 시스템 검증. 키움은 MVP 1로 이동.
3. **Idempotency 단순화**: `order_intent_id` 1회 생성 → 거기서 결정적 파생. strategy+가격+분 조합 폐기.
4. **`UNKNOWN_SUBMITTED` 상태 추가**: 네트워크 타임아웃 시 중복 주문 차단 + 조회 폴링.
5. **수치 타입 정책**: 가격·수량·금액은 `Decimal` 강제. `float` 사용 금지 영역 명시.
6. **DB 스키마 보강**: `accounts`, `cash_snapshots`, `position_snapshots`, `instruments` 추가 (Reconciliation 기반).
7. **Paper/Live 인프라 분리**: 코드 모드만이 아니라 .env·DB schema·Redis prefix·Vault path 전부 분리. 부팅 시 일관성 검증.
8. **BrokerCapabilities 모델**: 브로커 능력 차이를 코드로 표현. Risk Gate가 capability 보고 차단.
9. **수수료·규제 버전 관리**: `configs/fees/kr_2026.yaml` 형태로 연도별 분리. 백테스트 결과에 사용한 버전 기록.

### v3.0 (2026-05-27) — 초기 통합본
v2 보강 + 사용자 문서 통합. 33개 에이전트, YAML 전략, 3중 Risk Gate, MVP 0~6 로드맵.

---


---

## 목차

1. [설계 철학](#1-설계-철학)
2. [전체 아키텍처](#2-전체-아키텍처)
3. [브로커 추상화 계층](#3-브로커-추상화-계층)
4. [에이전트 카탈로그](#4-에이전트-카탈로그)
5. [데이터 흐름과 이벤트 모델](#5-데이터-흐름과-이벤트-모델)
6. [전략 엔진 (YAML 기반)](#6-전략-엔진-yaml-기반)
7. [Risk Gate (3중 안전망)](#7-risk-gate-3중-안전망)
8. [주문 상태 머신 & Idempotency](#8-주문-상태-머신--idempotency)
9. [Operating Mode & System State](#9-operating-mode--system-state)
10. [LLM 배치 전략](#10-llm-배치-전략)
11. [데이터 저장 구조](#11-데이터-저장-구조)
12. [백테스트 & 리플레이](#12-백테스트--리플레이)
13. [학습 루프](#13-학습-루프)
14. [ChatOps 인터페이스](#14-chatops-인터페이스)
15. [운영 인프라](#15-운영-인프라)
16. [보안 설계](#16-보안-설계)
17. [장애 대응 & Brownout](#17-장애-대응--brownout)
18. [규제·약관 체크](#18-규제약관-체크)
19. [레포 구조](#19-레포-구조)
20. [MVP 로드맵](#20-mvp-로드맵)
21. [부록: 핵심 인터페이스/스키마](#21-부록-핵심-인터페이스스키마)

---

## 1. 설계 철학

### 1.1 분리 기준

| 계층 | 책임 | 비결정성 허용? |
|---|---|---|
| **Agent Layer (LLM)** | 의견·해석·요약·근거 제시 | O (가능) |
| **Strategy Engine** | 룰/YAML/코드로 매매 후보 생성 | △ (제한적) |
| **Risk Engine** | 주문 전 차단 | **X (절대 결정론적)** |
| **Execution Engine** | 주문/정정/취소/체결 추적 | **X (절대 결정론적)** |
| **Broker Adapter** | API 차이 흡수 | X |

### 1.2 절대 원칙

1. **LLM은 주문 권한이 없다.** 모든 LLM 출력은 후보(candidate)일 뿐. 최종 발주는 결정론적 코드(Execution Agent)만 수행.
2. **Execution Engine은 LLM 없이도 100% 동작해야 한다.**
3. **모든 주문은 idempotency_key를 가진다.** 네트워크 재시도로 중복 주문이 나가지 않아야 함.
4. **Operating Mode는 디폴트로 `PAPER` 또는 `READ_ONLY`다.** 실전 자동(`LIVE_AUTO`)은 명시적 승격이 필요.
5. **Kill Switch는 항상 동작해야 한다.** 시스템 어느 부분이 죽어도 `/halt`는 작동.

### 1.3 토스 API 대응 전략

- 1단계: 키움 REST 모의투자로 전체 시스템 검증
- 2단계: 키움 실계좌 + KIS 미국주식
- 3단계: 토스 API 출시 시 `TossInvestAdapter`만 추가, 나머지 코드 변경 없음

이것이 가능하려면 `BrokerAdapter` 인터페이스를 처음부터 엄격하게 추상화해야 함.

---

## 2. 전체 아키텍처

### 2.1 다이어그램

```
┌─────────────────────────────────────────────────────────────┐
│ ChatOps: Telegram · Slack · Discord                          │
│ Reporter / Conversational Control / Daily Briefing           │
└──────────────────────────────┬──────────────────────────────┘
                               │ commands/notifications
┌──────────────────────────────▼──────────────────────────────┐
│ Control API (FastAPI) + Dashboard (Grafana)                 │
└──────────────────────────────┬──────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────┐
│ Event Bus (Redis Streams → 추후 NATS/Redpanda)               │
└─────────┬────────┬───────────────┬────────────┬─────────────┘
          │        │               │            │
          ▼        ▼               ▼            ▼
   Market    News/공시      Account/Fills    Macro Calendar
   Worker    Worker         Worker           Worker
          │        │               │            │
          └────────┴───────────────┴────────────┘
                          │
                          ▼
              ┌─────────────────────────┐
              │ Normalized Event Store   │
              │ (Postgres + TimescaleDB) │
              └────────────┬─────────────┘
                          │
       ┌──────────────────┴──────────────────┐
       │                                     │
       ▼ Analysis Layer (Local Qwen)         ▼ Context Layer
  News Analyst                          Market Regime
  Catalyst Hunter                       Macro Calendar
  Bear Case Agent                       Correlation/Sector
  Technical Signal Agent                Liquidity Agent
  Fundamental Agent                     Order Book Agent
  Verification Agent
       │                                     │
       └─────────────────┬───────────────────┘
                         ▼
            ┌─────────────────────────┐
            │ Strategy Engine          │
            │ - YAML rule-based        │
            │ - ML/LLM-assisted        │
            │ - Portfolio allocator    │
            │ - Knowledge Base (RAG)   │
            └────────────┬─────────────┘
                         │ order_intent
                         ▼
            ┌─────────────────────────┐
            │ Risk Gate (3중)          │
            │ 1. Risk Manager          │
            │ 2. Sanity Check          │
            │ 3. Compliance Agent      │
            └────────────┬─────────────┘
                         │
                  [Operating Mode]
        READ_ONLY → PAPER → LIVE_APPROVAL → LIVE_AUTO
                         │
                         ▼
            ┌─────────────────────────┐
            │ Execution Engine         │
            │ - State Machine          │
            │ - Idempotency            │
            │ - Order/Cancel/Modify    │
            └────────────┬─────────────┘
                         │
                         ▼
            ┌─────────────────────────────────┐
            │ Broker Adapter (Protocol)        │
            │ ├─ KiwoomRestKrMockAdapter       │
            │ ├─ KiwoomRestKrLiveAdapter       │
            │ ├─ KisOverseasAdapter (US)       │
            │ └─ TossInvestAdapter (future)    │
            └────────────┬─────────────────────┘
                         │ fills, positions
                         ▼
            ┌─────────────────────────┐
            │ Monitoring Layer         │
            │ - Position Monitor       │
            │ - Slippage Monitor       │
            │ - Reconciliation         │
            │ - Anomaly Detector       │
            │ - Strategy Drift         │
            └────────────┬─────────────┘
                         ▼
            ┌─────────────────────────┐
            │ Learning Loop            │
            │ Journal → Post-Mortem    │
            │ → Attribution → Drift    │
            │ → Improvement PRs        │
            └─────────────────────────┘
```

### 2.2 핵심 데이터 경로

`raw event → normalized event → analyzed signal → ranked candidate → order_intent → risk_check → broker_order → fill → reconciled position`

---

## 3. 브로커 추상화 계층

### 3.1 BrokerAdapter Protocol

```python
# core/brokers/base.py
from typing import Protocol, Literal, AsyncIterator
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime

Side = Literal["BUY", "SELL"]
OrderType = Literal["LIMIT", "MARKET"]
Market = Literal["KR", "US"]
TimeInForce = Literal["DAY", "IOC", "FOK"]


@dataclass(frozen=True)
class Symbol:
    market: Market
    code: str            # "005930" / "AAPL"
    currency: str        # "KRW" / "USD"


@dataclass(frozen=True)
class OrderRequest:
    account_id: str
    symbol: Symbol
    side: Side
    order_type: OrderType
    quantity: Decimal
    limit_price: Decimal | None
    time_in_force: TimeInForce
    strategy_id: str
    order_intent_id: str
    idempotency_key: str         # 중복 차단 핵심
    client_metadata: dict


@dataclass(frozen=True)
class OrderAck:
    broker: str
    broker_order_id: str
    order_intent_id: str
    status: str
    submitted_at: datetime
    raw_response: dict


@dataclass(frozen=True)
class Fill:
    broker_order_id: str
    symbol: Symbol
    side: Side
    quantity: Decimal
    price: Decimal
    fee: Decimal
    tax: Decimal
    filled_at: datetime


class BrokerAdapter(Protocol):
    name: str

    async def get_accounts(self) -> list[str]: ...
    async def get_cash(self, account_id: str) -> dict: ...
    async def get_positions(self, account_id: str) -> list[dict]: ...
    async def get_quote(self, symbol: Symbol) -> dict: ...
    async def get_orderbook(self, symbol: Symbol) -> dict: ...
    async def place_order(self, order: OrderRequest) -> OrderAck: ...
    async def cancel_order(self, account_id: str, broker_order_id: str) -> dict: ...
    async def modify_order(self, account_id: str, broker_order_id: str, new_qty: Decimal | None, new_price: Decimal | None) -> dict: ...
    async def get_order_status(self, account_id: str, broker_order_id: str) -> dict: ...
    async def stream_quotes(self, symbols: list[Symbol]) -> AsyncIterator[dict]: ...
    async def stream_fills(self, account_id: str) -> AsyncIterator[Fill]: ...
    async def is_tradable(self, symbol: Symbol) -> dict:
        """거래정지/관리종목/VI 등 체크"""
```

### 3.2 구현체 매핑 및 개발 순서

| 순서 | Adapter | 시장 | 환경 | 시점 | 목적 |
|---|---|---|---|---|---|
| **1** | **`SimulatedBrokerAdapter`** ★ | KR/US | 완전 로컬 | **MVP 0** | API 없이 시스템 검증 |
| 2 | `KiwoomRestKrMockAdapter` | KR | 모의 | MVP 1~ | 키움 API 첫 통합 |
| 3 | `KiwoomRestKrLiveAdapter` | KR | 실전 | MVP 3~ | |
| 4 | `KisOverseasMockAdapter` | US | 모의 | MVP 5 | |
| 5 | `KisOverseasLiveAdapter` | US | 실전 | MVP 5+ | |
| 6 | `TossInvestAdapter` | KR/US | 실전 | API 출시 후 | |

**왜 Simulated가 먼저인가**: 주문 상태 머신·idempotency·Risk Gate·`/halt`·Reconciliation은 외부 API 없이도 검증되어야 함. 처음부터 키움 API에 붙이면 API 이슈와 시스템 버그가 섞여 디버깅 불가능. Simulated는 다음을 시뮬레이션:
- 네트워크 타임아웃 (확률적)
- 부분 체결
- 주문 거부 (잔고 부족, 거래정지 등)
- 응답 지연
- 호가 잔량 기반 체결 가능성
- `UNKNOWN_SUBMITTED` 시나리오 (응답 받기 전 끊김)

### 3.3 BrokerCapabilities 모델 (★ 신규)

브로커마다 가능한 주문 방식이 다르다. Risk Gate와 Execution Engine은 capability를 보고 주문을 조정·차단한다.

```python
# brokers/capabilities.py
from dataclasses import dataclass
from decimal import Decimal

@dataclass(frozen=True)
class BrokerCapabilities:
    broker: str                              # 'kiwoom' | 'kis' | 'toss' | 'simulated'
    market: Market                           # 'KR' | 'US'
    environment: Literal["paper", "live"]
    
    # 주문 종류
    supports_market_order: bool
    supports_limit_order: bool
    supports_stop_order: bool
    supports_modify_order: bool
    supports_cancel_order: bool
    
    # 수량·가격
    supports_fractional_quantity: bool       # 미국 주식 fractional
    min_order_quantity: Decimal
    
    # 클라이언트 ID
    supports_client_order_id: bool           # idempotency_key 브로커 전달 가능 여부
    
    # 스트리밍
    supports_streaming_quotes: bool
    supports_streaming_orderbook: bool
    supports_streaming_fills: bool
    
    # 정정·취소
    supports_partial_cancel: bool
    cancel_after_partial_fill: bool
    
    # 시간 외
    supports_extended_hours: bool
    
    # Time in force
    supported_time_in_force: frozenset[TimeInForce]   # {DAY, IOC, FOK}
    
    # 한도
    max_requests_per_second: int | None
    max_orders_per_day: int | None
    
    # 거래 시간 (UTC)
    market_open_utc: tuple[int, int]         # (hour, minute)
    market_close_utc: tuple[int, int]


# 사용 예
KIWOOM_KR_MOCK_CAPS = BrokerCapabilities(
    broker="kiwoom",
    market="KR",
    environment="paper",
    supports_market_order=True,
    supports_limit_order=True,
    supports_stop_order=False,                # 키움은 OCO 없음
    supports_modify_order=True,
    supports_cancel_order=True,
    supports_fractional_quantity=False,       # 한국 주식 1주 단위
    min_order_quantity=Decimal("1"),
    supports_client_order_id=False,           # ★ 키움은 client_order_id 미지원 → 내부 매핑 필요
    supports_streaming_quotes=True,
    supports_streaming_orderbook=True,
    supports_streaming_fills=True,
    supports_partial_cancel=True,
    cancel_after_partial_fill=True,
    supports_extended_hours=False,
    supported_time_in_force=frozenset({"DAY"}),
    max_requests_per_second=5,
    max_orders_per_day=None,
    market_open_utc=(0, 0),                   # 09:00 KST = 00:00 UTC
    market_close_utc=(6, 30),                 # 15:30 KST = 06:30 UTC
)
```

**사용 패턴**:
```python
# Risk Gate가 주문 직전에 체크
if order.order_type == "MARKET" and not adapter.capabilities.supports_market_order:
    raise RiskBlock("Market order not supported by this broker")

if order.time_in_force not in adapter.capabilities.supported_time_in_force:
    # 자동 fallback or 차단
    ...

if not adapter.capabilities.supports_client_order_id:
    # idempotency_key를 브로커에 못 보내니까 내부 매핑 테이블 필수
    internal_mapping_table.record(order.order_intent_id, order.idempotency_key)
```

### 3.4 어댑터별 차이 흡수

각 어댑터가 흡수해야 할 차이:

- **인증**: OAuth2 (KIS) vs API key (키움) vs OCX (구 키움)
- **주문 단위**: 한국 주식은 1주, 미국 주식은 fractional 가능
- **호가 단위**: 가격대별 호가 단위 다름 (한국)
- **시장가/지정가**: 시장가 미지원 시장 대응
- **거래 시간**: KRX 09:00–15:30, NYSE 09:30–16:00 (서머타임)
- **수수료/세금**: 한국 매도 거래세, 미국 SEC fee
- **체결 통보**: WebSocket vs polling

---

## 4. 에이전트 카탈로그

### 4.1 전체 에이전트 표

| 계층 | 에이전트 | LLM | 주문권 | 주기 |
|---|---|---|---|---|
| **수집** | News Collector | 없음 | 불가 | 상시 |
| | Market Data Collector | 없음 | 불가 | 상시 |
| | Social Sentiment Collector | 없음 | 불가 | 5분 |
| | Macro Calendar Agent | 없음 | 불가 | 1일 |
| | Corporate Action Collector | 없음 | 불가 | 1일 |
| **정규화** | Entity Resolver (회사명→티커) | 낮음 | 불가 | 이벤트 |
| | Event Classifier | 중간 | 불가 | 이벤트 |
| **분석** | News Analyst | 높음 | 불가 | 이벤트 |
| | Catalyst Hunter | 높음 | 불가 | 이벤트 |
| | Bear Case Agent | 높음 | 불가 | 이벤트 |
| | Technical Signal Agent | 낮음 | 불가 | 분봉 |
| | Fundamental Agent | 중간 | 불가 | 1일 |
| | Verification Agent | 중간 | 불가 | 이벤트 |
| **컨텍스트** | Market Regime Agent | 낮음 | 불가 | 5분 |
| | Correlation/Sector Agent | 없음 | 불가 | 1일 |
| | Liquidity Agent | 없음 | 불가 | 1분 |
| | Order Book Agent | 없음 | 불가 | 실시간 |
| **의사결정** | Strategy Engine | 중간 | 시그널만 | 이벤트 |
| | Signal Ranker (LLM) | 중간~높음 | 시그널만 | 이벤트 |
| | **Decision Policy Engine** ★ | **없음 (코드)** | **order_intent 생성** | 이벤트 |
| **게이트** | Risk Manager | 없음 | 차단/승인 | 이벤트 |
| | Sanity Check Agent | 없음 | 차단 | 주문 직전 |
| | Compliance Agent | 없음 | 차단 | 주문 직전 |
| **실행** | Execution Agent | 없음 | **가능** | 이벤트 |
| | Slippage Monitor | 없음 | 불가 | 체결 후 |
| | Reconciliation Agent | 없음 | 불가 | 주기적 |
| **모니터링** | Position Monitor | 없음 | 손절/익절만 | 실시간 |
| | Anomaly Detector | 없음 | Brownout 트리거 | 상시 |
| | Strategy Drift Detector | 낮음 | 전략 비중 조정 | 1일 |
| **메타** | Performance Attribution | 낮음 | 불가 | 1주 |
| | Post-Mortem Agent | 높음 | 불가 | 1일 |
| | Journal / RAG Agent | 낮음 | 불가 | 상시 |
| **UI** | Reporter | 중간 | 불가 | 이벤트 |
| | Conversational Control | 중간 | 일부 조정 | 사용자 명령 |
| | Daily Briefing Agent | 높음 | 불가 | 1일 2회 |

### 4.2 에이전트별 핵심 책임 요약

#### 수집층

- **News Collector**: 네이버 금융·한경·연합인포맥스·Bloomberg/Reuters RSS·DART·EDGAR 수집, 중복 제거, 원문 저장.
- **Market Data Collector**: WebSocket 시세, 체결, 호가, OHLCV.
- **Macro Calendar Agent**: FOMC·CPI·NFP·옵션만기·어닝시즌 ±N시간 보수화 신호.
- **Corporate Action Collector**: 액면분할·배당락·무상증자 → 가격 조정.

#### 분석층 (Local Qwen 위주)

- **News Analyst**: 종목 매핑, 이슈 유형, 호재/악재, 시간 민감도.
- **Catalyst Hunter**: 급등 재료 후보 탐지.
- **Bear Case Agent**: 반대 논리, 함정 뉴스, 이미 반영된 호재.
- **Technical Signal Agent**: 차트·거래량·변동성·돌파/눌림 (코드 위주, LLM 보조).
- **Verification Agent**: 공시 원문 여부, 복수 출처, 재탕 기사 식별.

#### 컨텍스트층 ★

- **Market Regime Agent**: 시장 국면 분류 (bull/range/bear/panic/melt-up). 전략별 enable/disable.
- **Correlation/Sector Agent**: 보유 종목 간 상관관계, 섹터 중복 차단.
- **Liquidity Agent**: 본인 자본 대비 거래 가능 종목 필터.
- **Order Book Agent**: 호가 두께, 매물대, 스프레드 → 슬리피지 예측.

#### 게이트층

- **Risk Manager**: 계좌 한도, 비중, 일일 손실.
- **Sanity Check**: 수량·가격 이상치 (마지막 보루).
- **Compliance**: 규제·약관 (고빈도, 정정/취소 빈도, 미공개정보 종목).

#### 메타·학습층 ★

- **Performance Attribution**: 어느 전략·국면·에이전트가 +/- 였나.
- **Post-Mortem Agent**: 매일 손실 트레이드 재분석, 일지 누적.
- **Journal / RAG Agent**: 매매일지 벡터화 → 의사결정 시 "비슷한 과거 상황" 검색.
- **Strategy Drift Detector**: 백테스트 vs 라이브 성과 괴리 모니터링.

---

## 5. 데이터 흐름과 이벤트 모델

### 5.1 이벤트 타입

```python
# core/events/schemas.py
from pydantic import BaseModel
from datetime import datetime
from typing import Literal

class Event(BaseModel):
    event_id: str
    event_type: str
    occurred_at: datetime    # 사건 발생 시각 (UTC)
    ingested_at: datetime    # 시스템 수신 시각 (UTC)
    source: str
    schema_version: str

class NewsEvent(Event):
    event_type: Literal["news"] = "news"
    title: str
    body: str
    url: str
    published_at: datetime
    body_hash: str
    language: str

class MarketTickEvent(Event):
    event_type: Literal["market_tick"] = "market_tick"
    symbol_market: str
    symbol_code: str
    price: Decimal              # ★ float 금지 (금융 정밀도)
    volume: int
    bid: Decimal                # ★
    ask: Decimal                # ★
    currency: str               # KRW / USD

class SignalEvent(Event):
    event_type: Literal["signal"] = "signal"
    strategy_id: str
    symbol_market: str
    symbol_code: str
    side: Literal["BUY", "SELL"]
    confidence: float
    reason_json: dict
    expires_at: datetime | None

class OrderIntentEvent(Event):
    event_type: Literal["order_intent"] = "order_intent"
    order_intent_id: str
    strategy_id: str
    # ... OrderRequest fields ...

class FillEvent(Event):
    event_type: Literal["fill"] = "fill"
    broker_order_id: str
    # ... Fill fields ...

class RiskEvent(Event):
    event_type: Literal["risk"] = "risk"
    severity: Literal["INFO", "WARN", "BLOCK", "CRITICAL"]
    reason: str
    related_order_intent_id: str | None
```

### 5.1.1 수치 타입 원칙 (★ 중요)

금융 시스템에서 `float` 사용 금지 영역을 명시:

| 항목 | 타입 | 이유 |
|---|---|---|
| **가격** (price, bid, ask, limit_price) | **`Decimal`** | float는 0.1 + 0.2 ≠ 0.3 — 금융에서 치명적 |
| **수량** (quantity) | **`Decimal`** | 미국 fractional share 대응 |
| **금액** (cash, pnl, fee) | **`Decimal`** | 〃 |
| **비율** (confidence, score, pct) | `float` 허용 | 정밀도 영향 없음 |
| **카운트** (volume, trade_count) | `int` | |
| **시간** | `datetime` (UTC) | |

DB 컬럼: 가격·수량·금액은 `NUMERIC(20, 8)` 사용 (PostgreSQL).

대안 (더 엄격한 방식, 선택적): 모든 가격을 `price_minor_units: int` + `currency` + tick size rule로 저장. 처음엔 Decimal로 가는 것이 구현 부담 적음.

### 5.2 이벤트 버스

- **MVP 0~3**: Redis Streams
- **MVP 4+ (이벤트량 증가 시)**: NATS JetStream 또는 Redpanda

**스트림 토픽**:
```
events.news
events.market_tick
events.market_orderbook
events.macro_calendar
events.corporate_action
events.regime
events.signals
events.order_intents
events.risk
events.orders
events.fills
events.reconciliation
events.system_state
```

### 5.3 시간 처리 원칙

- **저장은 무조건 UTC.** ISO 8601 + microsecond.
- **표시할 때만 KST/EST 변환.**
- **거래 캘린더**: `exchange_calendars` 라이브러리 사용. 한국 휴장일, 미국 서머타임, 조기 종료일 자동 처리.
- **Clock 추상화**: 백테스트와 라이브에서 동일 인터페이스.

```python
# core/clock.py
class Clock(Protocol):
    def now(self) -> datetime: ...
    def is_market_open(self, market: str) -> bool: ...

class WallClock(Clock): ...        # 라이브용
class TickClock(Clock):            # 백테스트용
    def advance_to(self, t: datetime): ...
```

---

## 6. 전략 엔진 (YAML 기반)

### 6.1 자연어 → YAML → 코드 3단계

**1단계 (자연어 기록)**:
```
전략명: 뉴스 돌파 단타
- 시총 1천억 ~ 3조 중소형주
- 장중 호재성 뉴스
- 뉴스 후 5분 내 거래대금 평소 5배 이상
- 전고점 돌파 시 진입
- 첫 눌림에서 추가 진입 가능
- 단순 홍보성 뉴스 제외
- 이미 +15% 이상 급등한 종목 추격 금지
```

**2단계 (YAML 구조화)**:

```yaml
# configs/strategies/kr_news_breakout_v1.yaml
strategy_id: kr_news_breakout_v1
version: 1
market: KR
description: "장중 호재 뉴스 + 거래대금 급증 + 전고점 돌파 단타"

universe:
  market_cap:
    min_krw: 100_000_000_000
    max_krw: 3_000_000_000_000
  exclude:
    - trading_halt
    - management_issue
    - investment_warning
    - preferred_stock
    - spac
  liquidity:
    min_avg_value_20d_krw: 5_000_000_000

regime_filter:
  enabled_regimes: [bull_trend, bull_volatile, range]
  disabled_regimes: [bear_trend, panic]

entry:
  all_of:
    - news.sentiment_score >= 0.72
    - news.source_quality >= 0.6
    - news.age_minutes <= 5
    - news.verification_passed == true
    - volume.value_1m_krw >= avg_volume.value_1m_20d_krw * 5
    - price.breaks_intraday_high == true
    - price.change_from_open <= 0.15
    - orderbook.spread_pct <= 0.005

exit:
  stop_loss_pct: 0.025
  take_profit:
    - { pct: 0.04, qty_ratio: 0.5 }
    - { pct: 0.08, qty_ratio: 0.5 }
  time_stop_minutes: 45
  trailing_stop:
    activate_at_pct: 0.03
    trail_pct: 0.015

risk:
  max_position_pct_nav: 0.02
  max_daily_trades: 5
  max_strategy_daily_loss_pct_nav: 0.01
  max_concurrent_positions: 3

execution:
  order_type: limit
  limit_price_basis: best_ask
  max_slippage_pct: 0.003
  allow_market_order: false
  partial_fill_handling: cancel_remainder_after_minutes: 3

approval:
  mode: LIVE_APPROVAL    # MVP 3 단계
  auto_approve_when:
    - confidence_score >= 0.85
    - regime in [bull_trend]
```

**3단계 (코드)**: Strategy Engine이 YAML을 로드해서 시세·뉴스 이벤트에 매칭. 새 전략 추가는 YAML 파일 추가만으로 가능.

### 6.2 전략 카탈로그 (예시)

```
strategies/
  kr_news_breakout_v1.yaml       # 뉴스 돌파 단타
  kr_pullback_after_spike_v1.yaml # 급등 후 눌림목
  kr_opening_range_breakout.yaml  # 시초가 돌파
  kr_mean_reversion_v1.yaml       # 평균회귀 (코스피200)
  us_earnings_reaction_v1.yaml    # 어닝 반응
  us_gap_fill_v1.yaml             # 갭 메꿈
```

### 6.3 Portfolio Allocator

여러 전략이 동시에 신호를 낼 때 자본 배분:

```yaml
# configs/portfolio/allocator_v1.yaml
allocator:
  total_nav_pct_at_risk: 0.20      # 전체 자산 20%까지만 동시 노출
  per_strategy:
    kr_news_breakout_v1: 0.40
    kr_pullback_after_spike_v1: 0.30
    us_earnings_reaction_v1: 0.30
  cash_reserve_pct: 0.10
  per_market:
    KR: 0.60
    US: 0.40
  per_sector_max_pct: 0.30         # 한 섹터 30% 초과 금지
  fx_exposure_max_pct: 0.50        # 외화 노출
```

---

## 7. Risk Gate (3중 안전망)

모든 `order_intent`는 다음 3단계를 순차 통과해야 발주.

### 7.1 1단계: Risk Manager (계좌·전략 한도)

```yaml
# configs/risk/live_approval.yaml
global_risk:
  max_position_pct_nav: 0.02
  max_symbol_exposure_pct_nav: 0.03
  max_daily_loss_pct_nav: 0.01
  max_weekly_loss_pct_nav: 0.03
  max_monthly_loss_pct_nav: 0.08
  max_orders_per_day: 20
  max_orders_per_symbol_per_day: 3
  max_concurrent_positions: 10
  cash_reserve_pct: 0.10
  allow_short: false
  allow_margin: false
  allow_market_order: false
  require_manual_approval_for_live: true

per_market:
  KR:
    max_exposure_pct_nav: 0.60
    trading_hours_only: true
  US:
    max_exposure_pct_nav: 0.50
    extended_hours: false

per_strategy:
  kr_news_breakout_v1:
    max_daily_loss_pct_nav: 0.01
    max_concurrent_positions: 3
```

### 7.2 2단계: Sanity Check Agent (수량·가격 이상치)

마지막 보루. 결정론적 코드.

체크 항목:
- 주문 수량이 평소 평균의 10배 초과 → 차단
- 주문 가격이 현재가 ±5% 벗어남 → 차단
- 동일 종목 3분 안에 5번 주문 → 차단
- 단일 주문 금액이 NAV의 30% 초과 → 차단
- 거래정지·관리종목·VI 발동 종목 → 차단
- 일일 거래 횟수 한도 초과 → 차단
- API 재시도 중복 (idempotency_key 충돌) → 차단

### 7.3 3단계: Compliance Agent (규제·약관)

- 고빈도 거래 패턴 감지 (분당 N건 초과 시 경고/차단)
- 과도한 정정/취소 (정정률 30% 초과 시 차단)
- 본인 직장 미공개정보 종목 블랙리스트
- 한국 알고리즘 계좌 등록 의무 충족 여부
- 키움/KIS/토스 API 호출 한도 준수

### 7.4 통과 후 출력

```python
@dataclass
class RiskCheckResult:
    approved: bool
    order_intent_id: str
    risk_score: float        # 0.0 ~ 1.0
    blocking_rules: list[str]
    warnings: list[str]
    enriched_metadata: dict  # 슬리피지 추정 등
```

---

## 8. 주문 상태 머신 & Idempotency

### 8.1 상태 머신

```
SIGNAL_CREATED
   ↓
ORDER_INTENT_CREATED
   ↓
RISK_CHECK_PENDING
   ↓
RISK_APPROVED ─────┐         RISK_REJECTED → END
                   ↓
       [Operating Mode == LIVE_APPROVAL?]
                   ↓
   MANUAL_APPROVAL_PENDING ──→ APPROVED / REJECTED / EXPIRED
                   ↓
            ORDER_SUBMITTING       ★ 전송 중
                   ↓
        ┌──────────┴──────────┐
        ↓                     ↓
   BROKER_ACKED         UNKNOWN_SUBMITTED   ★ 타임아웃·응답 없음
        │                     ↓
        │              BROKER_STATUS_QUERYING
        │                     ↓
        │            ┌────────┼─────────┐
        │            ↓        ↓         ↓
        │      BROKER_ACKED  NOT_FOUND  MANUAL_REVIEW_REQUIRED
        │            │        │         │
        │            │     (재발주 가능) (사람 확인 필요)
        ↓            ↓
   ┌────┴────┐  ┌────┘
   ↓         ↓  ↓
PARTIALLY  FILLED       CANCELED   BROKER_REJECTED
FILLED       ↓             ↓            ↓
   ↓     RECONCILED    RECONCILED      END
FILLED       ↓             ↓
   ↓        END           END
RECONCILED
   ↓
  END
```

**`UNKNOWN_SUBMITTED` 처리 핵심 규칙**:
1. 브로커에 요청 보냈는데 응답 타임아웃 → `UNKNOWN_SUBMITTED`로 전이
2. 이 상태에서는 **같은 계좌·같은 종목·같은 방향의 신규 주문을 즉시 차단**
3. 브로커 주문 조회 API를 폴링 (지수 백오프, 같은 `order_intent_id`로)
4. 결과:
   - 주문 발견 → `BROKER_ACKED`로 전이, 정상 흐름
   - "해당 주문 없음" → `NOT_FOUND`, 재발주 가능
   - 모호함 (예: 일부 체결 흔적만 있음) → `MANUAL_REVIEW_REQUIRED`, 사람 개입
5. `MANUAL_REVIEW_REQUIRED`는 텔레그램 CRITICAL 알림 발송

### 8.2 ID 체계 및 Idempotency

**원칙**: 한 주문 의도(intent)마다 ID는 단 한 번만 생성하고, 모든 재시도는 같은 ID로만 한다.

```python
# core/execution/idempotency.py
import hashlib

# 1) signal_id: 시그널이 만들어질 때 부여
#    형식: SIG-{YYYYMMDD}-{MARKET}-{CODE}-{SEQ}
#    예:   SIG-20260527-KR-005930-000001

# 2) order_intent_id: signal → order_intent 변환 시 부여 (단 한 번)
#    형식: OI-{YYYYMMDD}-{MARKET}-{CODE}-{SEQ}
#    예:   OI-20260527-KR-005930-000001

# 3) idempotency_key: order_intent_id에서 결정적으로 파생
def make_idempotency_key(order_intent_id: str) -> str:
    return hashlib.sha256(order_intent_id.encode()).hexdigest()[:24]
```

**왜 강한 의도(strategy+symbol+qty+price+minute) 기반이 아닌가**:
- 같은 분에 같은 전략이 같은 조건으로 두 번 진입할 수 있음 → 충돌
- 재시도 중 가격이 미세하게 바뀌면 다른 key 생성 → 중복 주문 위험

**핵심 규칙**:
1. `order_intent_id`는 의도가 만들어질 때 **단 한 번** 생성
2. 모든 재시도·정정·취소는 **같은 `order_intent_id`** 사용
3. 브로커가 `duplicate_order` 반환 시 기존 주문 조회로 폴백
4. `order_intent_id`는 DB에 UNIQUE 제약

### 8.3 재시도 정책

- 네트워크 에러: 지수 백오프로 3회 재시도, 같은 `idempotency_key`로 재전송
- 브로커 200 OK 받기 전엔 절대 같은 의도로 새 idempotency_key 생성 금지
- 브로커가 `duplicate_order` 에러 반환 시 기존 주문 조회로 폴백

---

## 9. Operating Mode & System State

### 9.1 Operating Mode (사용자 의도)

| Mode | 신규 주문 | 시세 분석 | 알림 | 디폴트 |
|---|---|---|---|---|
| `READ_ONLY` | 없음 | O | O | ✓ (초기) |
| `PAPER` | 모의 계좌만 | O | O | ✓ (MVP 1~2) |
| `LIVE_APPROVAL` | 텔레그램 승인 후 | O | O | ✓ (MVP 3) |
| `LIVE_AUTO` | 검증된 전략만 자동 | O | O | (MVP 4+) |

승격은 명시적 명령: `/promote LIVE_AUTO --strategy kr_news_breakout_v1 --confirm`

### 9.1.1 Paper / Live 인프라 분리 (★ 매우 중요)

`Operating Mode`(코드 모드)만으로는 부족하다. 인프라 레벨에서도 분리되어야 한다.

```
환경 변수:
  .env.paper             paper 전용
  .env.live              live 전용 (별도 디렉토리, 별도 권한)

DB schema:
  trading_paper.*        paper 모드 전용 테이블
  trading_live.*         live 모드 전용 테이블

Redis stream prefix:
  paper.events.*
  live.events.*

Broker keys (Vault path):
  secret/brokers/kiwoom/paper/appkey
  secret/brokers/kiwoom/live/appkey
  secret/brokers/kis/paper/...
  secret/brokers/kis/live/...

Docker Compose:
  docker-compose.paper.yml
  docker-compose.live.yml
```

**강제 사항**:
1. paper 워커는 live 키에 접근 불가 (Vault 권한 분리)
2. live 워커는 `paper.events.*` 스트림 구독 불가
3. 시스템 부팅 시 `ENVIRONMENT` 변수 검증: 키와 스트림과 DB 스키마가 일치하지 않으면 즉시 종료
4. 단일 머신에서 paper와 live를 동시에 돌리지 말 것. 적어도 컨테이너 단위로 격리.

**부팅 시 검증 예시**:
```python
# core/bootstrap.py
def validate_environment_consistency():
    env = os.environ["ENVIRONMENT"]           # 'paper' | 'live'
    broker_key_env = vault.get_metadata(...)  # 키가 어느 환경 것인지
    db_schema = settings.db_schema             # trading_paper | trading_live
    
    if not (env == broker_key_env == db_schema.suffix):
        raise FatalConfigError(
            f"Environment mismatch: env={env}, key={broker_key_env}, db={db_schema}"
        )
```

### 9.2 System State (시스템 건강도)

| State | 의미 | 동작 |
|---|---|---|
| `NORMAL` | 모든 컴포넌트 정상 | 평상시 |
| `DEGRADED_NEWS` | 뉴스 수집 장애 | 뉴스 기반 전략 비활성, 룰 기반 계속 |
| `DEGRADED_LLM` | LLM 서버 장애 | LLM 호출 차단, 룰 기반만 |
| `DEGRADED_MARKET` | WebSocket 끊김 | REST 폴링 폴백, 신규 진입 차단 |
| `BROWNOUT` | 다중 컴포넌트 장애 | 보유 포지션 감시·손절만 |
| `HALTED` | 사용자 `/halt` | 신규 주문 0, 미체결 취소 |
| `EMERGENCY_STOP` | 일일 손실 한도 도달 등 | 모든 활동 중단, 사람 개입 필요 |

### 9.3 상태 전이 규칙

- `NORMAL → DEGRADED_*`: Heartbeat 실패, 에러율 임계 초과
- `DEGRADED_* → BROWNOUT`: 2개 이상 컴포넌트 동시 장애
- `* → HALTED`: 사용자 명령
- `* → EMERGENCY_STOP`: 일일 손실 한도, Anomaly Detector 트리거
- `EMERGENCY_STOP → *`: **사람의 명시적 명령으로만** 해제

---

## 10. LLM 배치 전략

### 10.1 로컬 vs 클라우드 분리

| 작업 | 위치 | 모델 | 이유 |
|---|---|---|---|
| 뉴스 1차 분류 | 로컬 | Qwen3.6-35B-A3B (MoE) | 양 많음, 비용 폭발 방지 |
| 종목명→티커 매핑 | 로컬 | Qwen3.6-35B-A3B | 양 많음 |
| 임베딩 생성 (RAG) | 로컬 | bge-m3 | 상시 |
| 공시 요약 | 로컬 | Qwen3.6-27B dense | 깊이 필요 |
| 시그널 후보 랭킹·근거 설명 | 로컬 또는 Claude | 상황별 | LLM은 랭킹·설명만, order_intent 생성은 결정론적 정책 엔진 |
| Daily Briefing | 클라우드 | Claude Sonnet | 글 품질 |
| Post-Mortem 일일 리뷰 | 클라우드 또는 로컬 야간 | 상황별 | 1일 1회 |
| Conversational Control | 클라우드 | Claude Sonnet | 응답 품질 |
| 개발 (전략 코드, 어댑터) | Claude Code / Codex | - | 개발 도구 |

### 10.2 3090 24GB VRAM 배분 (시나리오 C 추천)

```
Qwen3.6-35B-A3B Q4    →  20 GB   (메인: 뉴스·분석·종합)
bge-m3 embedding      →   2 GB   (RAG·유사도)
─────────────────────────────────
                         22 GB   (여유 2GB)
```

서빙: **vLLM** OpenAI-compatible endpoint (개발·운영 동일, `:8000/v1`)

### 10.3 LLM 출력 통제

- 모든 LLM 출력은 **JSON Schema 강제**
- 매매 권한 필드는 항상 `should_trade_directly: false`
- 출력 캐싱: `content_hash → response` (같은 뉴스 두 번 안 보내기)
- 프롬프트 버전 관리: `prompt_version`, `prompt_hash` 기록

### 10.4 비용·성능 추적

```sql
agent_runs
- id
- agent_name
- model_provider          -- 'local-qwen' | 'anthropic' | 'openai'
- model_name
- prompt_version
- prompt_hash
- input_refs              -- 입력 데이터 ID들
- output_json
- tokens_in
- tokens_out
- cost_usd
- cache_hit               -- 캐시 재사용 여부
- latency_ms
- created_at
```

월간 리포트: 에이전트별 토큰 사용량·비용·캐시 히트율.

---

## 11. 데이터 저장 구조

### 11.1 스토리지 선택

| 데이터 | 스토리지 |
|---|---|
| 시세·OHLCV·체결 | **PostgreSQL + TimescaleDB** |
| 이벤트 큐 / 캐시 | **Redis Streams + Redis** |
| 원문 뉴스·공시 HTML/PDF | **MinIO / S3-compatible** |
| LLM 프롬프트·응답 원본 | **MinIO + 인덱스는 Postgres** |
| 매매일지·전략 설명 임베딩 | **Qdrant** 또는 **pgvector** |
| 메트릭 | **Prometheus** |
| 대시보드 | **Grafana** |
| 로그 | **Loki** 또는 파일 + S3 백업 |

### 11.2 핵심 테이블

```sql
-- 계좌 (paper/live 명시적 분리)
accounts (
  id TEXT PRIMARY KEY,
  broker TEXT,
  market TEXT,
  environment TEXT,              -- 'paper' | 'live'
  base_currency TEXT,
  created_at TIMESTAMPTZ
);

-- 현금 스냅샷 (시점별)
cash_snapshots (
  id UUID PRIMARY KEY,
  account_id TEXT REFERENCES accounts(id),
  currency TEXT,
  cash_available NUMERIC(20, 8),
  cash_total NUMERIC(20, 8),
  captured_at TIMESTAMPTZ,
  source TEXT                    -- 'broker_query' | 'internal_calc'
);

-- 포지션 스냅샷 (Reconciliation의 기준)
position_snapshots (
  id UUID PRIMARY KEY,
  account_id TEXT REFERENCES accounts(id),
  symbol_market TEXT,
  symbol_code TEXT,
  quantity NUMERIC(20, 8),
  avg_price NUMERIC(20, 8),
  market_price NUMERIC(20, 8),
  unrealized_pnl NUMERIC(20, 8),
  realized_pnl NUMERIC(20, 8),
  captured_at TIMESTAMPTZ,
  source TEXT                    -- 'broker_query' | 'internal_calc'
);

-- 종목 마스터 (호가 단위·거래 가능 여부 등)
instruments (
  symbol_market TEXT,
  symbol_code TEXT,
  name TEXT,
  currency TEXT,
  exchange TEXT,
  tick_size_rule JSONB,          -- 가격대별 호가 단위
  lot_size NUMERIC(20, 8),
  is_tradable BOOLEAN,
  is_halted BOOLEAN,
  is_managed BOOLEAN,            -- 관리종목
  market_cap_krw NUMERIC(20, 0),
  sector TEXT,
  updated_at TIMESTAMPTZ,
  PRIMARY KEY (symbol_market, symbol_code)
);

-- 뉴스 원문
news_items (
  id UUID PRIMARY KEY,
  source TEXT,
  url TEXT,
  title TEXT,
  body_hash TEXT,
  published_at TIMESTAMPTZ,
  collected_at TIMESTAMPTZ,
  raw_text TEXT,
  language TEXT,
  s3_key TEXT
);

-- 정규화 이벤트
normalized_events (
  id UUID PRIMARY KEY,
  event_type TEXT,
  symbol_market TEXT,
  symbol_code TEXT,
  sentiment_score NUMERIC,
  catalyst_score NUMERIC,
  risk_score NUMERIC,
  source_quality NUMERIC,
  event_time TIMESTAMPTZ,
  evidence_json JSONB
);

-- 시그널
signals (
  id UUID PRIMARY KEY,
  strategy_id TEXT,
  symbol_market TEXT,
  symbol_code TEXT,
  side TEXT,
  confidence NUMERIC,
  expected_horizon TEXT,
  reason_json JSONB,
  created_at TIMESTAMPTZ
);

-- 주문 의도
order_intents (
  id TEXT PRIMARY KEY,           -- order_intent_id
  idempotency_key TEXT UNIQUE,
  strategy_id TEXT,
  symbol_market TEXT,
  symbol_code TEXT,
  side TEXT,
  quantity NUMERIC,
  limit_price NUMERIC,
  status TEXT,
  risk_result_json JSONB,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ
);

-- 실제 주문
orders (
  id UUID PRIMARY KEY,
  broker TEXT,
  broker_order_id TEXT,
  order_intent_id TEXT REFERENCES order_intents(id),
  status TEXT,
  submitted_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  raw_response JSONB
);

-- 체결
fills (
  id UUID PRIMARY KEY,
  broker_order_id TEXT,
  symbol_market TEXT,
  symbol_code TEXT,
  side TEXT,
  quantity NUMERIC,
  price NUMERIC,
  fee NUMERIC,
  tax NUMERIC,
  filled_at TIMESTAMPTZ
);

-- 에이전트 실행 기록
agent_runs (
  id UUID PRIMARY KEY,
  agent_name TEXT,
  model_provider TEXT,
  model_name TEXT,
  prompt_version TEXT,
  prompt_hash TEXT,
  input_refs JSONB,
  output_json JSONB,
  tokens_in INT,
  tokens_out INT,
  cost_usd NUMERIC,
  cache_hit BOOLEAN,
  latency_ms INT,
  created_at TIMESTAMPTZ
);

-- 리스크 이벤트
risk_events (
  id UUID PRIMARY KEY,
  severity TEXT,
  reason TEXT,
  related_order_intent_id TEXT,
  metadata_json JSONB,
  created_at TIMESTAMPTZ
);

-- 시스템 상태 변경
system_state_log (
  id UUID PRIMARY KEY,
  from_state TEXT,
  to_state TEXT,
  reason TEXT,
  triggered_by TEXT,           -- 'system' | 'user' | agent_name
  created_at TIMESTAMPTZ
);

-- 일일 성과
daily_performance (
  date DATE,
  market TEXT,
  realized_pnl NUMERIC,
  unrealized_pnl NUMERIC,
  trade_count INT,
  win_rate NUMERIC,
  max_drawdown_pct NUMERIC,
  attribution_json JSONB,
  PRIMARY KEY (date, market)
);
```

TimescaleDB 하이퍼테이블 후보: `normalized_events`, `fills`, market tick 저장 테이블.

---

## 12. 백테스트 & 리플레이

### 12.1 두 종류의 백테스트

**A. OHLCV 백테스트** — 빠르고 가벼움. 룰 기반 전략 초기 검증용.

**B. 이벤트 리플레이 백테스트** — 뉴스·공시·시세를 시간순으로 재생. LLM 에이전트 포함 전체 시스템 검증용.

```python
# scripts/replay_day.py
async def replay_day(date: date, strategy_ids: list[str]):
    clock = TickClock(start=date_start_utc(date))
    bus = ReplayEventBus()
    
    # 그 날짜의 모든 이벤트 시간순 로드
    events = load_events_by_date(date)
    
    # 시스템 초기화 (그 날짜 시작 시점의 계좌 상태)
    portfolio = load_snapshot(date_start_utc(date))
    
    for event in events:
        clock.advance_to(event.occurred_at)
        await bus.publish(event)
        # 에이전트들이 이벤트를 처리하고 주문 생성
        # 주문은 SimulatedBrokerAdapter가 체결 시뮬레이션
        # 슬리피지·수수료·세금 반영
```

### 12.2 백테스트 함정 방지

- **Look-ahead bias**: 모든 이벤트는 `as_of` 타임스탬프로 필터링. 미래 데이터 절대 금지.
- **Survivorship bias**: 상장폐지된 종목도 유니버스에 포함. KRX·NYSE 과거 상장 종목 리스트 보존.
- **체결 가능성**: 호가 잔량 무시한 체결 금지. 한 번에 호가 잔량의 N% 이하만 체결로 처리.
- **수수료·세금·환율**: 한국 매도 거래세 (0.18% 코스피, 0.20% 코스닥, 2026년 기준 확인 필요), 미국 SEC fee, 원달러 환율.
- **LLM 프롬프트 버전 고정**: 백테스트 시 그 날짜의 `prompt_hash` 사용. 프롬프트 바뀌면 다른 실험으로 취급.
- **Walk-forward 검증**: 1년치를 6개월 학습 / 3개월 검증 / 3개월 out-of-sample로 분할.

### 12.3 백테스트 결과 메트릭

- 누적 수익률, CAGR
- 샤프 비율, 소르티노 비율
- 최대 낙폭 (MDD)
- 승률, 손익비
- 거래 횟수, 평균 보유 시간
- 슬리피지·수수료 비중
- 국면별 성과 (Regime breakdown)

---

## 13. 학습 루프

### 13.1 사이클

```
매 트레이드 → Journal 기록
       ↓
일일 (장 마감 후) Post-Mortem Agent (Claude/Local 야간)
   - 손실 트레이드 재분석
   - "어느 시그널을 무시했어야 했나"
   - 비슷한 과거 패턴 검색 (RAG)
   - Journal에 누적
       ↓
주간 Performance Attribution
   - 전략별 / 국면별 / 에이전트별 +/-
   - "kr_news_breakout: bull_trend에서 +12%, range에서 -3%"
       ↓
월간 Strategy Drift Review
   - 백테스트 대비 라이브 성과 괴리
   - 괴리 큰 전략은 자동으로 비중 축소
       ↓
프롬프트·전략 파라미터 개선 PR (사람이 리뷰 후 머지)
```

### 13.2 Journal/RAG 스키마

```python
# Vector DB (Qdrant) collection
journal_entries:
  - id: uuid
  - trade_id: uuid
  - strategy_id: str
  - symbol: str
  - entry_at: datetime
  - exit_at: datetime
  - pnl: float
  - pnl_pct: float
  - regime_at_entry: str
  - signals_used: list[str]
  - news_refs: list[str]
  - narrative: str             # LLM이 생성한 트레이드 요약
  - lessons: str               # LLM이 생성한 교훈
  - embedding: vector(1024)    # narrative + lessons 임베딩
```

새 의사결정 시 현재 상황을 임베딩해서 비슷한 과거 트레이드를 RAG 검색 → 컨텍스트로 주입.

### 13.3 자동 개선 금지

- 학습 루프는 **제안**만 함
- 프롬프트·전략 YAML 변경은 **사람이 PR 리뷰 후 머지**
- 자동 파라미터 튜닝은 백테스트에서만 허용, 라이브에서 자동 변경 금지

---

## 14. ChatOps 인터페이스

### 14.1 명령어 카탈로그

```
/status                          시스템 전체 상태
/mode                            현재 Operating Mode
/mode set PAPER                  모드 변경
/promote LIVE_AUTO --strategy=X  전략별 LIVE_AUTO 승격
/halt                            전체 자동매매 중단
/resume_paper                    모의투자만 재개
/positions                       보유 종목 + 평가손익
/today                           오늘 매매 요약
/watch                           감시 중인 종목
/why <SYMBOL>                    왜 보고 있는지 설명
/approve <ORDER_INTENT_ID>       주문 승인 (LIVE_APPROVAL)
/reject <ORDER_INTENT_ID>        주문 거절
/risk                            현재 리스크 한도 사용량
/strategies                      활성 전략 목록 + 성과
/disable <STRATEGY_ID>           특정 전략 비활성
/regime                          현재 시장 국면
/logs                            최근 에러/경고
/briefing                        Daily Briefing 즉시 생성
/journal <DATE>                  특정 날짜 매매일지
```

### 14.2 알림 등급

| 레벨 | 채널 | 예시 |
|---|---|---|
| `INFO` | Slack #info | 매수 후보, 시그널, 일일 요약 |
| `ALERT` | Slack #alert + Telegram | 주문 차단, Drift 감지 |
| `CRITICAL` | Telegram + SMS (선택) | EMERGENCY_STOP, 잔고 불일치 |

### 14.3 알림 예시

**매수 후보 (INFO)**:
```
[매수 후보] KR 005930 삼성전자
전략: kr_news_breakout_v1
점수: 0.81
국면: bull_trend
뉴스: 실적 전망 상향 보도
차트: 장중 고점 돌파 + 1분 거래대금 20일 평균 대비 6.2배
리스크: 단일종목 비중 1.7%, 스프레드 0.08%
RAG: 과거 비슷한 상황 3건 평균 +4.2% (45분 보유)

제안: 지정가 78,500원 × 12주
손절: -2.5% / 익절: +4.0%, +8.0% / 시간손절: 45분

상태: 수동 승인 대기 (만료 2분)
명령어: /approve KR-NEWS-BREAKOUT-20260527-005930-000123
```

**리스크 차단 (ALERT)**:
```
[주문 차단] KR 123456
전략: kr_news_breakout_v1
사유:
- 뉴스 발생 후 이미 +18.4% 상승 (entry rule 위반)
- 스프레드 1.2% 초과
- 오늘 동일 전략 손실 한도 82% 사용
결론: 주문하지 않음
```

**EMERGENCY_STOP (CRITICAL)**:
```
🚨 EMERGENCY_STOP 발동
시각: 2026-05-27 14:23 KST
사유: 일일 손실 한도 도달 (-1.2% > -1.0%)
조치: 모든 신규 주문 중단, 미체결 자동 취소
보유: 3종목 유지 (모니터링만)
재개: 사람의 명시적 명령 필요
명령어: /resume_paper
```

### 14.4 보안

- 명령 실행 가능 user_id allowlist
- `/approve`, `/halt`, `/promote`, `/resume_*` 등 위험 명령은 2분 만료 토큰
- 모든 명령은 `audit_log` 테이블에 append-only 기록

---

## 15. 운영 인프라

### 15.1 호스트 사양

```
Host: Ubuntu 22.04/24.04
  - NVIDIA Driver + Docker + NVIDIA Container Toolkit
  - RTX 3090 24GB
  - 64GB+ RAM 권장
  - NVMe SSD 1TB+
  - 안정적 인터넷 회선 (브로커 API·뉴스 수집)
```

### 15.2 컨테이너 그룹화 (5계층) ★

컨테이너는 단순 나열이 아니라 **5개 그룹**으로 분류해서 관리한다. 그룹마다 정책이 다르다.

| 그룹 | 컨테이너 | 재시작 정책 | 헬스체크 | 의존성 | 죽으면? |
|---|---|---|---|---|---|
| **Infra** | postgres, redis, minio, qdrant | `unless-stopped` | TCP probe | 없음 | 전체 다운, 즉시 알림 |
| **GPU** | llm-server, embedding-server | `unless-stopped` | HTTP `/health` | infra | LLM 전략 비활성, 룰 기반은 계속 |
| **Data** | worker-market-*, worker-news, worker-macro | `always` | Redis heartbeat | infra | 해당 데이터 소스만 DEGRADED |
| **Decision** | worker-agent, worker-strategy, worker-decision | `always` | Redis heartbeat | infra (+ GPU 선택적) | 신규 진입 차단, 보유 감시는 계속 |
| **Critical** | worker-execution-*, worker-monitor, api, bot-telegram | `always` + 우선순위 최상 | Redis heartbeat + 자체 watchdog | **infra만** ★ | 시스템 사실상 마비 |

**Critical 그룹의 특별 규칙**:
1. **infra에만 의존**. GPU·Data·Decision이 모두 죽어도 Critical은 살아있어야 함
2. worker-monitor는 다른 워커 죽었을 때도 **결정론적 규칙으로 보유 포지션 손절** 처리
3. bot-telegram은 다른 워커 죽었을 때도 `/halt`, `/positions` 응답해야 함
4. Critical 그룹은 자체 watchdog 보유 (10초 무응답 시 자동 재시작)

### 15.3 MVP 단계별 컨테이너 (점진적 추가)

#### MVP 0 — 8개 (외부 API 0개)

```
[Infra]
  postgres, redis, minio

[Critical]
  api, worker-execution, worker-monitor, bot-telegram

[기타]
  worker-market   (이때는 SimulatedBrokerAdapter가 시세도 시뮬레이션)
```

**목적**: SimulatedBrokerAdapter로 풀 사이클 검증. 외부 의존 0.

#### MVP 1 — 9개 (키움 통합)

MVP 0 + 추가:
- `worker-market`를 키움 모의 API 시세로 전환 (컨테이너 수는 그대로, 어댑터만 교체)
- 운영 데이터 늘어나면 `prometheus`, `grafana` 추가 (+2)

#### MVP 2 — 14개 (LLM + 뉴스)

```
[Infra] +1
  + qdrant                    (벡터 DB / RAG)

[GPU] +2
  + llm-server                (vLLM + Qwen3.6-35B-A3B)
  + embedding-server          (bge-m3)

[Data] +1
  + worker-news               (뉴스/공시 수집)

[Decision] +2
  + worker-agent              (LLM 분석)
  + worker-strategy           (전략 매칭)
```

#### MVP 4 — 18개 내외 (실전 + 분리)

```
[Data] 분리
  worker-market → worker-market-kr + worker-market-us
  + worker-macro              (매크로 캘린더)

[Decision] +1
  + worker-decision           (Decision Policy Engine — 결정론적)

[Critical] 분리
  worker-execution → worker-execution-kr + worker-execution-us

[Monitoring] +1
  + loki                      (로그 집계)
```

20개 넘어가면 docker-compose 한계 → Kubernetes 또는 Nomad 이전 고려.

### 15.4 분리·합침 규칙 ★

#### 절대 합치면 안 되는 조합

| A | B | 이유 |
|---|---|---|
| worker-execution | worker-agent | LLM 호출 지연(수 초)이 주문 지연으로 전파 |
| worker-monitor | 다른 워커 | monitor는 마지막 보루, 다른 게 죽어도 살아야 함 |
| bot-telegram | api | 둘 다 죽으면 사람이 시스템에 못 들어감 |
| llm-server | embedding-server | 호출 패턴 다름 (큰 호출 vs 끊임없는 작은 호출), 임베딩이 LLM 뒤에 밀림 |
| worker-execution-kr | worker-execution-us | 시장별 격리 (한 시장 API 장애가 다른 시장 영향 X) |
| postgres | redis | 둘 다 인프라지만 장애 양상 완전히 다름 |
| worker-news | worker-market | 뉴스는 외부 크롤링이라 rate limit·네트워크 장애 자주, 시세는 안정적이어야 함 |

#### 합쳐도 되는 경우

| 상황 | 합치는 대상 | 단, |
|---|---|---|
| MVP 0 개발 초기 | 모든 worker를 하나로 (`worker-all`) | MVP 1 진입 시 즉시 분리 |
| 전략이 1~2개뿐 | worker-strategy를 worker-execution에 포함 | MVP 2 이후 분리 필수 |
| 모니터링 도구 | prometheus + grafana 같은 compose 그룹 | 자원은 분리 |

#### 분리의 3가지 신호

다음 중 하나라도 발생하면 분리 검토:
1. **장애 전파**: A 컨테이너 죽었을 때 B 기능까지 못 쓰게 됨
2. **자원 경합**: CPU/RAM/GPU를 같이 잡아서 한쪽이 느려짐
3. **재배포 주기 차이**: A는 매일 배포, B는 한 달에 한 번 → 같이 묶으면 B가 매일 재시작당함

### 15.5 자원 할당 가이드 (3090 1장 + 64GB RAM)

| 컨테이너 | RAM | GPU | CPU | 비고 |
|---|---|---|---|---|
| postgres | 8 GB | - | 2 | `shared_buffers=2GB`, TimescaleDB extension |
| redis | 2 GB | - | 1 | maxmemory 1.5GB, eviction policy=allkeys-lru |
| minio | 2 GB | - | 1 | |
| qdrant | 4 GB | - | 2 | (MVP 2+) |
| llm-server | 8 GB | 18~20 GB | 4 | vLLM + Qwen3.6-35B-A3B Q4 |
| embedding-server | 4 GB | 2~3 GB | 2 | bge-m3 |
| worker-market-* | 1~2 GB | - | 1 | WebSocket 유지 |
| worker-news | 2 GB | - | 1 | HTTP 크롤링 + 파싱 |
| worker-macro | 1 GB | - | 0.5 | |
| worker-agent | 2 GB | - | 1 | LLM 클라이언트 |
| worker-strategy | 2 GB | - | 1 | |
| worker-decision | 1 GB | - | 0.5 | 순수 코드, 가벼움 |
| worker-execution-* | 2 GB | - | 1 | |
| worker-monitor | 2 GB | - | 1 | |
| api | 1 GB | - | 1 | FastAPI |
| bot-telegram | 0.5 GB | - | 0.5 | |
| prometheus | 2 GB | - | 1 | 메트릭 보존 기간 따라 |
| grafana | 1 GB | - | 0.5 | |
| loki | 2 GB | - | 1 | (MVP 4+) |
| **OS + 여유** | **8 GB** | - | - | |
| **합계 (MVP 4)** | **~50 GB** | **22 GB** | **~20** | 64GB RAM·24GB VRAM 머신 적정 |

**GPU 공유 전략**: llm-server와 embedding-server는 같은 3090을 공유. 컨테이너만 분리, NVIDIA Container Toolkit이 메모리 격리.

```yaml
# 예시 — GPU 메모리 제한
llm-server:
  deploy:
    resources:
      reservations:
        devices:
          - driver: nvidia
            capabilities: [gpu]
  environment:
    - CUDA_VISIBLE_DEVICES=0
    - VLLM_GPU_MEMORY_UTILIZATION=0.85   # 20.4GB까지

embedding-server:
  deploy:
    resources:
      reservations:
        devices:
          - driver: nvidia
            capabilities: [gpu]
  environment:
    - CUDA_VISIBLE_DEVICES=0
    - GPU_MEMORY_FRACTION=0.10           # 2.4GB까지
```

### 15.6 Paper / Live Docker Compose 분리

§9.1.1의 인프라 분리 원칙을 docker compose 단위에서 구현.

```
infra/
  docker-compose.base.yml      # 인프라 공통 (postgres, redis, ...)
  docker-compose.paper.yml     # paper 모드 워커들
  docker-compose.live.yml      # live 모드 워커들
  docker-compose.dev.yml       # 개발용 (mailhog, adminer 등 추가)
  .env.paper                   # paper 환경 변수
  .env.live                    # live 환경 변수
```

**실행**:
```bash
# Paper 환경
docker compose -f docker-compose.base.yml -f docker-compose.paper.yml --env-file .env.paper up -d

# Live 환경 (별도 머신 권장)
docker compose -f docker-compose.base.yml -f docker-compose.live.yml --env-file .env.live up -d
```

**단일 머신에서 paper와 live 동시 실행 금지**. 적어도 별도 네트워크와 별도 볼륨으로 격리.

### 15.7 헬스체크 & 재시작 정책

#### docker-compose 헬스체크 예시

```yaml
services:
  worker-execution:
    image: trading/worker-execution:latest
    healthcheck:
      test: ["CMD", "python", "-m", "core.healthcheck", "execution"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 30s
    restart: always
    depends_on:
      postgres: { condition: service_healthy }
      redis:    { condition: service_healthy }

  worker-monitor:
    # Critical 그룹 — 더 엄격하게
    healthcheck:
      test: ["CMD", "python", "-m", "core.healthcheck", "monitor", "--strict"]
      interval: 5s              # 더 자주
      timeout: 3s
      retries: 2
      start_period: 20s
    restart: always
    stop_grace_period: 30s      # graceful shutdown 시간 확보
```

#### 워커 내부 heartbeat

각 워커는 Redis에 5초마다 heartbeat 기록:
```python
# 모든 워커 공통
await redis.set(f"heartbeat:{worker_name}", iso_now(), ex=15)
```

worker-monitor가 이걸 보고 죽은 워커를 텔레그램 알림.

#### 재시작 정책

| 그룹 | 정책 | 이유 |
|---|---|---|
| Infra | `unless-stopped` | 사람이 의도적으로 멈춘 게 아니면 항상 켜져있어야 |
| GPU | `unless-stopped` | 〃 |
| Data | `always` | 일시적 외부 장애 후 자동 복구 |
| Decision | `always` | 〃 |
| Critical | `always` + watchdog | 절대 죽어선 안 됨 |

### 15.8 다중 머신 (확장 시)

단일 머신 18개 넘으면:

```
[GPU 머신]
  llm-server, embedding-server

[Trading 머신]
  Critical + Decision + Data 그룹 워커들

[Infra 머신 또는 매니지드]
  postgres (RDS), redis (ElastiCache), minio (S3), qdrant
```

머신 간 통신은 VPN 또는 private subnet. Critical과 GPU 머신이 분리되면 LLM 장애 시 자동 폴백 더 깔끔.

### 15.9 모니터링 메트릭

- **시스템**: CPU/GPU/메모리/디스크 I/O/네트워크
- **컨테이너**: 컨테이너별 RAM·CPU 사용량, 재시작 횟수, OOM kill 여부
- **애플리케이션**: 이벤트 처리 지연, Redis Stream 큐 길이, 워커 heartbeat
- **비즈니스**: 일일 PnL, 거래 횟수, 승률, 평균 슬리피지, LLM 비용·캐시 히트율
- **알람 임계치**:
  - Critical 그룹 워커 down 10초 → Telegram CRITICAL
  - Redis Stream 큐 길이 > 1000 → Slack ALERT
  - LLM 응답 지연 > 5초 (p95) → Slack ALERT
  - 일일 LLM 비용 > 예산 → Slack ALERT

---

## 16. 보안 설계

### 16.1 비밀 관리

- API 키 절대 Git 커밋 금지 (`.gitignore` 강제, `pre-commit` 훅)
- 운영 비밀: **HashiCorp Vault** / **SOPS + age** / **Doppler**
- `.env`는 로컬 개발만, 운영 서버는 Vault에서 주입
- paper 키와 live 키 완전 분리
- 가능하면 조회 전용 키와 주문 키 분리

### 16.2 권한 분리

- 개발 서버 ↔ 운영 서버 네트워크 분리
- SSH key-only, 패스워드 로그인 금지
- 운영 서버는 outbound 화이트리스트 (브로커 도메인만)

### 16.3 감사 로그

```sql
audit_log (
  id UUID PRIMARY KEY,
  actor TEXT,            -- user_id or 'system' or agent_name
  action TEXT,           -- 'order_approve', 'halt', 'mode_change'
  target TEXT,           -- order_intent_id, strategy_id
  channel TEXT,          -- 'telegram', 'api', 'auto'
  payload JSONB,
  created_at TIMESTAMPTZ
);
```

Append-only. 삭제·수정 금지. 분기별 백업.

### 16.4 텔레그램 봇 보안

```yaml
telegram:
  bot_token: vault://secret/telegram/bot_token
  allowed_user_ids:
    - <your_telegram_id>
  dangerous_commands:
    - /approve
    - /halt
    - /promote
    - /resume_live
    - /disable
  approval_token_ttl_seconds: 120
  rate_limit: 30_per_minute
```

---

## 17. 장애 대응 & Brownout

### 17.1 Kill Switch 계층

| 레벨 | 트리거 | 동작 |
|---|---|---|
| **L1** Strategy disable | `/disable kr_news_breakout_v1` | 해당 전략만 중단 |
| **L2** Symbol disable | `/disable_symbol 005930` | 해당 종목 거래 중단 |
| **L3** Market disable | `/disable_market US` | 시장 단위 중단 |
| **L4** Halt | `/halt` | 신규 주문 0, 미체결 취소 |
| **L5** Emergency Stop | 자동 트리거 | 모든 활동 중단, 사람 개입 |

### 17.2 장애 감지 트리거

- 시세 데이터 지연 ≥ 3초
- WebSocket heartbeat 끊김 ≥ 10초
- 브로커 주문 응답 지연 ≥ 5초
- 잔고 조회 실패 3회 연속
- 체결과 내부 포지션 불일치
- LLM 응답 파싱 실패율 ≥ 10%/분
- 일일 손실 한도 도달
- 같은 종목 같은 방향 주문 30초 안에 N번 (오작동 의심)

### 17.3 Brownout 동작

- `DEGRADED_NEWS`: 뉴스 기반 전략 비활성, 룰 기반 계속
- `DEGRADED_LLM`: LLM 호출 차단, 캐시된 결과만 사용, 룰 기반만
- `DEGRADED_MARKET`: REST 폴링 폴백, 신규 진입 차단, 보유 감시·손절은 유지
- `BROWNOUT`: 보유 포지션 감시·손절만, 신규 진입 0
- `HALTED`: 모든 신규 주문 0, 미체결 취소
- `EMERGENCY_STOP`: 모든 활동 중단, 사람 명시적 명령으로만 해제

### 17.4 복구 절차

`EMERGENCY_STOP` 발동 시 체크리스트:
1. 잔고 수동 확인 (브로커 앱)
2. 내부 포지션 DB와 대조
3. 미체결 주문 수동 취소 확인
4. 손실 원인 분석 (Post-Mortem)
5. 시스템 재시작 전 `Reconciliation Agent` 강제 실행
6. `/resume_paper`로 일단 모의로 전환, 24시간 관찰
7. 이상 없으면 `/mode set LIVE_APPROVAL`

---

## 18. 규제·약관 체크

### 18.1 한국 (KRX)

- 알고리즘 거래자 등록 의무 (일정 거래량 이상)
- 호가 정정/취소 횟수 모니터링
- 시세 유인 행위 금지 (반복 주문 후 취소)
- 미공개정보 이용 금지
- 키움/한투/토스 API 호출 한도 준수
- 양도소득세 트래킹 (해외주식, 비상장 등)

### 18.2 미국 (US)

- Pattern Day Trader (PDT) 규정 (25k 미만 계좌 데이트레이드 제한)
- Wash sale 룰 (손실 인식 제한)
- SEC fee, FINRA fee 반영
- W-8BEN 갱신

### 18.3 시스템 강제 사항

수수료·세금·규제 값은 **절대 코드에 하드코딩하지 않는다.** 시간이 지나면 바뀌므로 버전 관리된 YAML로 분리.

```
configs/
  fees/
    kr_2026.yaml
    kr_2027.yaml         # 새 연도 나오면 새 파일
    us_2026.yaml
  compliance/
    kr_rules_2026.yaml
    us_rules_2026.yaml
```

**예시 — 수수료 config**:

```yaml
# configs/fees/kr_2026.yaml
version: 2026.05
effective_from: 2026-01-01
effective_until: null            # 다음 버전 나올 때까지

stocks:
  buy:
    commission_pct: 0.00015      # 거래 수수료 0.015% (예시값, 실제 확인 필요)
  sell:
    commission_pct: 0.00015
    transaction_tax_pct:
      KOSPI: 0.0018              # 코스피 매도 거래세 (확인 필요)
      KOSDAQ: 0.0020              # 코스닥 매도 거래세 (확인 필요)
    agriculture_tax_pct:
      KOSPI: 0.0015              # (확인 필요)
```

```yaml
# configs/compliance/kr_rules_2026.yaml
version: 2026.05
effective_from: 2026-01-01

kr:
  max_orders_per_minute: 30
  max_cancel_rate_pct: 30
  forbidden_patterns:
    - spoofing
    - layering
    - momentum_ignition
  algorithmic_trader:
    threshold_orders_per_day: 1000   # 등록 의무 기준 (확인 필요)
    registered: false
```

**백테스트 결과에 fee/compliance 버전 기록**:

```sql
backtest_runs (
  id UUID PRIMARY KEY,
  strategy_id TEXT,
  start_date DATE,
  end_date DATE,
  fees_config_version TEXT,        -- "kr_2026.05"
  compliance_config_version TEXT,
  llm_prompt_version TEXT,
  result_json JSONB,
  created_at TIMESTAMPTZ
);
```

이게 없으면 "이 백테스트는 어떤 수수료·규제로 돌린 건지" 사후 추적 불가.

### 18.4 Compliance Agent 동작

Compliance Agent가 매 주문마다 위 규칙들을 강제. 룰셋 로딩은 부팅 시 1회 + 핫리로드 지원.

---

## 19. 레포 구조

```
trading-agent/
├── apps/
│   ├── api/                          # FastAPI control plane
│   │   ├── main.py
│   │   └── routes/
│   ├── worker_market/                # 시세 수집
│   ├── worker_news/                  # 뉴스·공시 수집
│   ├── worker_agent/                 # LLM 분석
│   ├── worker_strategy/              # 전략 실행
│   ├── worker_execution/             # 주문 실행
│   ├── worker_monitor/               # 포지션·이상감지
│   └── bot_telegram/
│
├── core/
│   ├── events/
│   │   ├── schemas.py
│   │   └── bus.py
│   ├── models/
│   │   ├── market.py
│   │   ├── order.py
│   │   └── portfolio.py
│   ├── clock.py                      # ★ Tick/Wall Clock 추상화
│   ├── operating_mode.py             # ★ READ_ONLY/PAPER/LIVE_*
│   ├── system_state.py               # ★ NORMAL/DEGRADED/BROWNOUT
│   ├── risk/
│   │   ├── gate.py
│   │   ├── limits.py
│   │   ├── sanity_check.py
│   │   └── compliance.py
│   ├── execution/
│   │   ├── engine.py
│   │   ├── state_machine.py
│   │   └── idempotency.py
│   └── strategies/
│       ├── base.py
│       ├── registry.py
│       └── yaml_loader.py
│
├── brokers/
│   ├── base.py                       # BrokerAdapter Protocol
│   ├── capabilities.py               # ★ BrokerCapabilities 모델
│   ├── simulated.py                  # ★ MVP 0 (외부 API 없음)
│   ├── kiwoom_rest_kr_mock.py        # MVP 1~
│   ├── kiwoom_rest_kr_live.py        # MVP 3
│   ├── kis_overseas_mock.py          # MVP 5
│   ├── kis_overseas_live.py          # MVP 5+
│   └── toss_invest_future.py         # API 출시 후
│
├── agents/
│   ├── collectors/
│   │   ├── news_collector.py
│   │   ├── market_data.py
│   │   ├── social_sentiment.py
│   │   ├── macro_calendar.py
│   │   └── corporate_action.py
│   ├── analysts/
│   │   ├── news_analyst.py
│   │   ├── catalyst_hunter.py
│   │   ├── bear_case.py
│   │   ├── technical_signal.py
│   │   ├── fundamental.py
│   │   └── verification.py
│   ├── context/
│   │   ├── market_regime.py          # ★
│   │   ├── correlation_sector.py     # ★
│   │   ├── liquidity.py              # ★
│   │   └── order_book.py             # ★
│   ├── decision/
│   │   ├── strategy_engine.py
│   │   └── decision_engine.py
│   ├── monitoring/
│   │   ├── position_monitor.py
│   │   ├── slippage_monitor.py
│   │   ├── reconciliation.py
│   │   ├── anomaly_detector.py       # ★
│   │   └── strategy_drift.py         # ★
│   ├── meta/
│   │   ├── post_mortem.py
│   │   ├── performance_attribution.py # ★
│   │   └── knowledge_base/           # ★ RAG
│   │       ├── indexer.py
│   │       └── retriever.py
│   ├── ui/
│   │   ├── reporter.py
│   │   ├── conversational_control.py # ★
│   │   └── daily_briefing.py         # ★
│   └── prompts/
│       ├── news_analyst_v1.md
│       ├── catalyst_hunter_v1.md
│       └── ...
│
├── configs/
│   ├── strategies/
│   │   ├── kr_news_breakout_v1.yaml
│   │   ├── kr_pullback_after_spike_v1.yaml
│   │   ├── kr_opening_range_breakout.yaml
│   │   └── us_earnings_reaction_v1.yaml
│   ├── risk/
│   │   ├── paper.yaml
│   │   ├── live_approval.yaml
│   │   └── live_auto.yaml
│   ├── regime/
│   │   └── regime_v1.yaml            # ★
│   ├── compliance/
│   │   ├── kr_rules_2026.yaml        # ★ 연도별 버전 관리
│   │   └── us_rules_2026.yaml        # ★
│   ├── fees/                         # ★ 신규
│   │   ├── kr_2026.yaml
│   │   └── us_2026.yaml
│   ├── portfolio/
│   │   └── allocator_v1.yaml
│   └── llm/
│       ├── routing.yaml              # 어느 에이전트가 어느 모델 쓸지
│       └── prompts/
│
├── data/
│   ├── corporate_actions/            # ★ 액면분할·배당락
│   ├── market_calendars/             # ★ 휴장일
│   └── universe/                     # 거래 가능 종목 리스트
│
├── infra/
│   ├── docker-compose.yml
│   ├── docker-compose.dev.yml
│   ├── prometheus.yml
│   ├── grafana/
│   │   └── dashboards/
│   └── vault/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── replay/                       # 이벤트 리플레이 테스트
│   └── fixtures/
│
├── scripts/
│   ├── backtest.py
│   ├── replay_day.py
│   ├── reconcile_positions.py
│   ├── daily_postmortem.py           # ★
│   ├── weekly_attribution.py         # ★
│   └── monthly_drift_review.py       # ★
│
├── docs/
│   ├── SYSTEM_DESIGN.md              # 이 문서
│   ├── BROKER_API_NOTES.md
│   ├── STRATEGY_AUTHORING_GUIDE.md
│   └── RUNBOOK.md                    # 장애 대응 절차서
│
├── .gitignore
├── pyproject.toml
└── README.md
```

---

## 20. MVP 로드맵

### MVP 0 (1주) — 골격 (외부 API 없음)

**목표**: 외부 API 0개로 주문 의도 → Risk Gate → 시뮬레이션 체결 → DB 기록 풀 사이클

- 레포 구조, `pyproject.toml`, Docker Compose
- Postgres + TimescaleDB + Redis 띄우기
- `BrokerAdapter` Protocol 정의 + **`BrokerCapabilities`** 모델
- **`SimulatedBrokerAdapter`** 구현 (네트워크 지연·타임아웃·부분체결·`UNKNOWN_SUBMITTED` 시나리오 포함)
- `OperatingMode` 상태 머신 (`READ_ONLY` 디폴트)
- 주문 상태 머신 (`UNKNOWN_SUBMITTED` 포함 풀 전이)
- Idempotency 매니저 (order_intent_id 기반 해시)
- Telegram 봇 `/status`, `/halt`만
- 감사 로그 테이블
- **`paper`/`live` 환경 분리** (.env, DB 스키마, Redis prefix)

**완료 기준**:
1. `/status` → 시뮬 계좌 잔고 표시
2. 수동 트리거로 시뮬 주문 발주 → Risk Gate 통과 → 체결 → 포지션 반영
3. `UNKNOWN_SUBMITTED` 시나리오에서 중복 주문 안 나감 (테스트)
4. `/halt` 동작 확인

### MVP 1 (2주) — 키움 통합 + 룰 기반 전략

**목표**: 실제 키움 모의투자 API 연동 + 룰 기반 전략 1개 가동

- `KiwoomRestKrMockAdapter` 구현 (인증·시세·주문·체결·잔고·WebSocket)
- 키움 capability 등록
- Market Data Collector (시세·체결·호가)
- TimescaleDB OHLCV 저장
- Corporate Action Collector (액면분할·배당락)
- 단순 룰 전략 1개 YAML (`kr_opening_range_breakout`)
- Strategy Engine (YAML 로더 + 매칭)
- **Decision Policy Engine** (결정론적 order_intent 생성)
- Risk Manager + Sanity Check
- OHLCV 백테스트 엔진
- `PAPER` 모드 가동

**완료 기준**: 키움 모의 계좌에서 전략 1개가 24시간 돌고, 일일 리포트 텔레그램 수신.

### MVP 2 (3주) — 뉴스·국면 추가

**목표**: 뉴스 기반 의사결정 + 시장 국면 인식

- vLLM + Qwen3.6 세팅
- News Collector + Entity Resolver
- News Analyst + Catalyst Hunter + Bear Case (Local Qwen)
- Verification Agent
- **Market Regime Agent** ★
- **Liquidity Agent** ★ (유니버스 정의)
- Macro Calendar Agent
- 뉴스 기반 전략 1개 YAML (`kr_news_breakout_v1`)
- JSON Schema 강제 + 출력 캐싱

**완료 기준**: 장중 호재 뉴스 → 5분 내 매수 후보 텔레그램 알림. PAPER 모드에서 자동 발주.

### MVP 3 (2주) — 수동 승인 자동매매

**목표**: 실전 소액 + 수동 승인 (`LIVE_APPROVAL`)

- 키움 실전 REST 어댑터
- `LIVE_APPROVAL` 모드
- Conversational Control (`/approve`, `/reject`, `/why`)
- Slippage Monitor
- Reconciliation Agent
- Daily Briefing Agent (장 시작 전, 마감 후)
- 일일·주간 손실 한도 강제

**완료 기준**: 실계좌 (소액) + 수동 승인. 2주간 무사고.

### MVP 4 (2주) — 부분 자동

**목표**: 검증된 1~2개 전략만 `LIVE_AUTO`

- `/promote` 명령으로 전략별 자동 승격
- Anomaly Detector + Brownout 모드
- Post-Mortem 일일 자동 (Claude)
- Performance Attribution (주간)
- EMERGENCY_STOP 자동 트리거
- Compliance Agent 풀 가동

**완료 기준**: 1~2개 전략이 LIVE_AUTO. 일일·주간 자동 리포트. 한 번도 EMERGENCY_STOP 트리거 안 됨.

### MVP 5 (3~4주) — 미국주식 + RAG

**목표**: 멀티마켓 + 학습 루프

- `KisOverseasAdapter` (US)
- 환율 처리, US 캘린더
- 미국 전략 1개 (`us_earnings_reaction_v1`)
- Strategy Drift Detector
- Knowledge Base / RAG (Journal + Qdrant)
- 월간 Drift Review 자동

**완료 기준**: 한국·미국 동시 운영. RAG가 의사결정에 컨텍스트 제공.

### MVP 6+ — 토스 API + 고도화

- `TossInvestAdapter` (API 출시 시)
- 전략 카탈로그 확장
- Social Sentiment Collector (선택)
- Fundamental Agent 추가 (스윙·장기)
- 다중 머신 분산 (필요 시)

---

## 21. 부록: 핵심 인터페이스/스키마

### 21.1 LLM 출력 JSON Schema 예시 (News Analyst)

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["symbol_candidates", "event_type", "sentiment", "should_trade_directly"],
  "properties": {
    "symbol_candidates": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["market", "code", "name", "confidence"],
        "properties": {
          "market": {"enum": ["KR", "US"]},
          "code": {"type": "string"},
          "name": {"type": "string"},
          "confidence": {"type": "number", "minimum": 0, "maximum": 1}
        }
      }
    },
    "event_type": {
      "enum": ["earnings", "guidance", "contract", "fda", "policy",
               "m_and_a", "buyback", "dilution", "lawsuit", "rumor",
               "promotion", "other"]
    },
    "sentiment": {"enum": ["positive", "negative", "neutral", "mixed"]},
    "sentiment_score": {"type": "number", "minimum": -1, "maximum": 1},
    "catalyst_score": {"type": "number", "minimum": 0, "maximum": 1},
    "time_sensitivity": {"enum": ["immediate", "intraday", "swing", "long"]},
    "source_quality": {"type": "number", "minimum": 0, "maximum": 1},
    "summary": {"type": "string", "maxLength": 500},
    "bull_case": {"type": "array", "items": {"type": "string"}},
    "bear_case": {"type": "array", "items": {"type": "string"}},
    "should_trade_directly": {"const": false},
    "required_checks": {"type": "array", "items": {"type": "string"}}
  }
}
```

### 21.2 catalyst_score 가중치 (튜닝 가능)

```yaml
# configs/scoring/catalyst_v1.yaml
catalyst_score:
  weights:
    source_quality: 0.25
    novelty_score: 0.20            # 재탕 기사 여부
    financial_materiality: 0.20    # 실적 영향도
    time_sensitivity: 0.15
    sector_momentum: 0.10
    price_volume_confirmation: 0.10
  thresholds:
    consider: 0.55
    strong: 0.72
    very_strong: 0.85
```

### 21.3 LLM 라우팅 규칙 및 권한 경계

**핵심**: LLM은 절대 `order_intent`를 생성하지 않는다. 후보 랭킹과 설명만 한다.

```yaml
# configs/llm/routing.yaml
roles:
  decision_engine:
    role: candidate_ranking_and_explanation
    primary: local-qwen-35b-a3b
    fallback: claude-sonnet
    may_create_signal: true
    may_create_order_intent: false      # ★ LLM은 order_intent 생성 불가
    may_place_order: false
    require_json_schema: true

  final_decision_policy:
    role: deterministic_policy_engine    # ★ 순수 코드, LLM 없음
    may_create_order_intent: true
    inputs:
      - strategy_signal
      - risk_limits
      - market_state
      - portfolio_state
      - broker_capabilities
      - regime_state

routing:
  news_analyst:
    primary: local-qwen-35b-a3b
    fallback: claude-haiku
    cache_ttl_seconds: 3600

  catalyst_hunter:
    primary: local-qwen-35b-a3b
    fallback: claude-haiku

  signal_ranker:                         # 시그널 랭킹 (의사결정 X)
    primary: claude-sonnet
    fallback: local-qwen-27b
    require_json_schema: true

  daily_briefing:
    primary: claude-sonnet

  post_mortem:
    primary: claude-sonnet
    schedule: "after_market_close"

  conversational_control:
    primary: claude-sonnet

  embedding:
    primary: local-bge-m3
```

**흐름 명확화**:
```
LLM agents → signal (랭킹·점수·설명)
                ↓
DecisionPolicyEngine (순수 코드) → order_intent
                ↓
Risk Gate (코드) → approved order_intent
                ↓
Execution Engine (코드) → broker order
```

LLM은 `signal` 까지만 만들 수 있다. `order_intent`부터는 결정론적 코드만.

### 21.4 RUNBOOK 항목 (별도 문서)

- 장 시작 전 체크리스트 (08:30 KST)
- 장 마감 후 체크리스트 (15:40 KST)
- EMERGENCY_STOP 복구 절차
- 잔고 불일치 발생 시 절차
- LLM 서버 다운 시 절차
- 브로커 API 장애 시 절차
- 일일 손실 한도 도달 시 절차

---

## 마치며

이 설계의 **핵심 원칙**을 다시 강조:

1. **LLM은 의견, 코드는 결정.** 매매 권한은 결정론적 코드에만.
2. **3중 Risk Gate.** 모든 주문은 Risk Manager → Sanity Check → Compliance를 통과.
3. **Operating Mode 단계적 승격.** READ_ONLY → PAPER → LIVE_APPROVAL → LIVE_AUTO. 디폴트는 안전한 쪽.
4. **Brownout으로 부분 장애 대응.** Kill Switch만 있으면 과도하게 멈춤.
5. **Broker는 어댑터, 코어는 불변.** 토스 API는 어댑터만 추가.
6. **모든 결정 근거 기록.** Journal → Post-Mortem → Attribution → Drift → 개선.
7. **백테스트는 이벤트 리플레이까지.** OHLCV만으론 LLM 전략 검증 불가능.
8. **비용은 측정 가능해야 한다.** agent_runs에 토큰·비용 기록.

**다음 작업 후보**:
- (a) `core/operating_mode.py` + `system_state.py` 실제 구현 (안전망 골격)
- (b) `brokers/base.py` + `kiwoom_rest_kr_mock.py` 실제 구현 (첫 주문)
- (c) 본인 매매 스킬 → YAML 전략 변환 워크숍

권장 순서: **(a) → (b) → (c)**.
안전망 → 진짜 주문 → 본인 스킬 이전.
