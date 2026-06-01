# Stock MVP Trading System

## 1. 시스템 개요

한/미 주식 자동매매 시스템입니다.
MVP 0~6 단계로 구성되며, 브로커·전략·리스크·모니터링을 모듈화해 실전/모의 모드를 분리합니다.

- 데이터 수집: 시세/뉴스/기관 이벤트/사회 신호(플레이스홀더)
- 전략 판단: YAML 기반 규칙, LLM 신호 보조, 결정은 코드로 고정
- 주문 실행: BrokerAdapter 추상화 + 위험 관리 + 실행 상태 머신
- 운영 안전장치: 모드/상태 머신, 손실 제한, reconciliation

## 2. MVP 로드맵 요약

- **MVP 0**: 골격 구축, Simulated Broker, Risk Gate, Paper pipeline
- **MVP 1**: 키움 모의 API 통합 + 룰 기반 1개 전략
- **MVP 2**: 뉴스/국면/감시 파이프라인
- **MVP 3**: 수동 승인 모드(LIVE_APPROVAL), 텔레그램 승인
- **MVP 4**: 부분 자동화 및 자동 승인 단계
- **MVP 5**: 미국시장 통합, 다중 시장 실행
- **MVP 6+**: 토스 API 어댑터, 소셜 감성, 고도화

## 3. 빠른 시작

```bash
cp .env.paper.example .env.paper
docker compose -f docker-compose.mvp0.yml --env-file .env.paper up -d
```

주요 단계:

- `docker compose -f docker-compose.mvp0.yml ps`
- `docker compose -f docker-compose.mvp0.yml logs -f worker-execution`
- 종료: `docker compose -f docker-compose.mvp0.yml down`

## 4. 환경 변수 설명

- `ENVIRONMENT`: `paper` 또는 `live`
- `APP_MODE`: `READ_ONLY`/`PAPER`/`LIVE_APPROVAL`/`LIVE_AUTO`
- `OPERATING_MODE`: 내부 승인 정책 모드
- `BROKER_ADAPTER`: `simulated` / `kiwoom_rest_kr_mock` / `kiwoom_rest_kr_live` / `kis_overseas_*` / `toss_invest_future`(플레이스홀더)
- `REDIS_URL`: Redis URL
- `REDIS_STREAM_PREFIX`: 이벤트 스트림 prefix (예: `paper.events`)
- `DATABASE_URL`: Postgres connection string
- `LLM_API_URL`, `LLM_MODEL`, `LLM_FALLBACK_MODEL`: 전략 신호 LLM 호출
- `KIWOOM_*`, `KIS_*`: 브로커 인증 키 (해당 모드에서 사용)
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_ALLOWED_USER_IDS`: ChatOps
- `DART_API_KEY`: 뉴스 수집 보조
- `EMBEDDING_API_URL`, `QDRANT_URL`: 지식기반(RAG) 연동

## 5. 전략 추가 방법

1. `configs/strategies/<strategy_id>.yaml` 생성
2. `strategy_id`, `market`, `universe`, `regime_filter`, `entry/exit`, `risk`, `execution`, `approval` 작성
3. `configs/portfolio/allocator_v1.yaml`에 strategy 가중치 반영
4. `scripts/backtest_ohlcv.py`로 백테스트 후 승인
5. PAPER에서 smoke run, 이상 시 승인/리스크 로그 확인

참고: `docs/STRATEGY_AUTHORING_GUIDE.md`에 상세 문법이 정리되어 있다.

## 6. ChatOps 명령어 목록

- ` /status `: 운영 상태, 포지션, 최근 체결 요약 조회
- ` /halt `: 수동 중단
- ` /resume `: 수동 정지 해제
- ` /approve <order_intent_id> `: 대기 주문 승인
- ` /reject <order_intent_id> `: 대기 주문 거절
- ` /why <symbol> `: 최근 감시 사유 조회
- ` /risk `: 일일/주간 리스크 사용률 조회
