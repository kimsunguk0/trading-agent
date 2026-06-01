# 전략 YAML 작성 가이드

## 1) `universe` 필드

- `market`: KR / US를 지정한다.
- `index`: KOSPI200 등 지수 기준 스크리닝이 필요할 때 사용한다.
- `market_cap`, `market_cap_usd`: 지역 통화 기준 시장가치 최소/최대 조건.
- `liquidity`: 거래량/거래대금 기반 필터.
- `exclude`: 거래 금지/주의 종목 코드 태그(예: `trading_halt`, `investment_warning`).

## 2) `entry` / `exit` 조건 문법

모든 조건은 `all_of` 목록의 불리언 식으로 작성한다.

- 비교 연산자: `>=`, `<=`, `==`, `!=`, `<`, `>`
- 불리언 비교: `== true`, `== false`
- 네임스페이스형 피처: `price.*`, `volume.*`, `news.*`, `indicators.*`
- 예시:

```yaml
entry:
  all_of:
    - price.gap_down_pct >= 0.02
    - volume.open_30min_vs_avg >= 2.0
    - news.sentiment_score >= 0.6
```

`exit`는 크게 두 그룹으로 작성한다.

- 손절/익절 구간
  - `stop_loss_pct`
  - `take_profit`: 다단계 익절 (`pct`, `qty_ratio`)
- 시간/조건 종료
  - `time_stop_minutes`

## 3) `regime_filter` 사용법

- `enabled_regimes`: 전략이 허용되는 국면만 나열.
- `disabled_regimes`: 예외 국면.
- 충돌 시 `disabled`가 우선한다.
- 전략별 국면 가중치 보정이 필요한 경우 운영 정책/포트폴리오 레이어에서 별도 반영한다.

## 4) `risk` 파라미터 의미

- `max_position_pct_nav`: 전략 단일 포지션 최대 비중
- `max_daily_trades`: 일별 주문 빈도 상한
- `max_strategy_daily_loss_pct_nav`: 전략 단위 일일 손실 한도
- `max_concurrent_positions`: 동시 보유/미청산 포지션 상한

### 권장 규칙

- 유사 전략 간 과적합 방지를 위해 같은 시장 노출의 누적비중을 별도 allocator에서 관리한다.
- 일중 급등락 구간에서는 `max_daily_trades`를 낮게 두고 과도한 롤링 노출을 방지한다.

## 5) `approval` 모드별 동작

- `PAPER`: 체계 내 시뮬레이션/백테스트 기반 실행(리스크 가드 통과 시)
- `LIVE_APPROVAL`: 주문 생성 후 수동 승인 필요 (`/approve`, `/reject` 플로우)
- `LIVE_AUTO`: 허용된 전략만 자동 실행

각 모드에서 동일 YAML이라도 운영 상태 전이에 따라 실제 실행 가능성이 달라진다.

## 6) 백테스트 방법

1. 전략 YAML을 `configs/strategies/`에 추가
2. 동일 규칙을 `scripts/backtest_ohlcv.py` 실행 입력으로 변환
3. 수익률, 최대 낙폭, 승률, 체결률, 슬리피지 감도 점검
4. `configs/portfolio/allocator_v1.yaml` 및 `configs/scoring/catalyst_v1.yaml`과 함께 이벤트-기반 통합 시뮬레이션 수행
5. 실제 운영 전 PAPER/Live-Approval에서 최소 2회 이상 회귀 테스트
