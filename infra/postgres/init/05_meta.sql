-- MVP 4 meta tables: journal_entries, performance_attribution, strategy_drift_log
-- Created for both trading_paper and trading_live schemas.

-- ─────────────────────────────────────────────────────────────────────────────
-- trading_paper
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS trading_paper.journal_entries (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_id         UUID,
    strategy_id      TEXT,
    symbol_market    TEXT,
    symbol_code      TEXT,
    entry_at         TIMESTAMPTZ,
    exit_at          TIMESTAMPTZ,
    pnl              NUMERIC(20,8),
    pnl_pct          NUMERIC(10,6),
    regime_at_entry  TEXT,
    signals_used     JSONB,
    news_refs        JSONB,
    narrative        TEXT,
    lessons          TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_paper_journal_strategy
    ON trading_paper.journal_entries (strategy_id);
CREATE INDEX IF NOT EXISTS idx_paper_journal_exit_at
    ON trading_paper.journal_entries (exit_at);

CREATE TABLE IF NOT EXISTS trading_paper.performance_attribution (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    period_start     DATE,
    period_end       DATE,
    strategy_id      TEXT,
    regime           TEXT,
    realized_pnl     NUMERIC(20,8),
    trade_count      INT,
    win_rate         NUMERIC(10,6),
    sharpe           NUMERIC(10,6),
    attribution_json JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_paper_perf_attr_strategy
    ON trading_paper.performance_attribution (strategy_id, period_start);

CREATE TABLE IF NOT EXISTS trading_paper.strategy_drift_log (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id         TEXT,
    backtest_sharpe     NUMERIC(10,6),
    live_sharpe         NUMERIC(10,6),
    sharpe_diff         NUMERIC(10,6),
    backtest_win_rate   NUMERIC(10,6),
    live_win_rate       NUMERIC(10,6),
    action_taken        TEXT,
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_paper_drift_strategy
    ON trading_paper.strategy_drift_log (strategy_id, detected_at);

-- ─────────────────────────────────────────────────────────────────────────────
-- trading_live
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS trading_live.journal_entries (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_id         UUID,
    strategy_id      TEXT,
    symbol_market    TEXT,
    symbol_code      TEXT,
    entry_at         TIMESTAMPTZ,
    exit_at          TIMESTAMPTZ,
    pnl              NUMERIC(20,8),
    pnl_pct          NUMERIC(10,6),
    regime_at_entry  TEXT,
    signals_used     JSONB,
    news_refs        JSONB,
    narrative        TEXT,
    lessons          TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_live_journal_strategy
    ON trading_live.journal_entries (strategy_id);
CREATE INDEX IF NOT EXISTS idx_live_journal_exit_at
    ON trading_live.journal_entries (exit_at);

CREATE TABLE IF NOT EXISTS trading_live.performance_attribution (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    period_start     DATE,
    period_end       DATE,
    strategy_id      TEXT,
    regime           TEXT,
    realized_pnl     NUMERIC(20,8),
    trade_count      INT,
    win_rate         NUMERIC(10,6),
    sharpe           NUMERIC(10,6),
    attribution_json JSONB,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_live_perf_attr_strategy
    ON trading_live.performance_attribution (strategy_id, period_start);

CREATE TABLE IF NOT EXISTS trading_live.strategy_drift_log (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy_id         TEXT,
    backtest_sharpe     NUMERIC(10,6),
    live_sharpe         NUMERIC(10,6),
    sharpe_diff         NUMERIC(10,6),
    backtest_win_rate   NUMERIC(10,6),
    live_win_rate       NUMERIC(10,6),
    action_taken        TEXT,
    detected_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_live_drift_strategy
    ON trading_live.strategy_drift_log (strategy_id, detected_at);
