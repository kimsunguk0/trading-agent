CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS trading_paper.ohlcv (
    bucket_start TIMESTAMPTZ NOT NULL,
    market TEXT NOT NULL,
    symbol TEXT NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bucket_start, market, symbol)
);

CREATE INDEX IF NOT EXISTS idx_paper_ohlcv_symbol_bucket ON trading_paper.ohlcv (symbol, bucket_start DESC);

CREATE TABLE IF NOT EXISTS trading_paper.corporate_actions (
    corporate_action_id BIGSERIAL PRIMARY KEY,
    market TEXT NOT NULL,
    symbol TEXT NOT NULL,
    action_type TEXT NOT NULL,
    title TEXT,
    cash_amount NUMERIC(20, 8),
    shares_per_stock NUMERIC(20, 8),
    ratio NUMERIC(20, 8),
    as_of TIMESTAMPTZ NOT NULL,
    raw_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (market, symbol, action_type, as_of, title)
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('trading_paper.ohlcv', 'bucket_start', if_not_exists => TRUE);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS trading_live.ohlcv (
    bucket_start TIMESTAMPTZ NOT NULL,
    market TEXT NOT NULL,
    symbol TEXT NOT NULL,
    open_price NUMERIC(20, 8) NOT NULL,
    high_price NUMERIC(20, 8) NOT NULL,
    low_price NUMERIC(20, 8) NOT NULL,
    close_price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bucket_start, market, symbol)
);

CREATE INDEX IF NOT EXISTS idx_live_ohlcv_symbol_bucket ON trading_live.ohlcv (symbol, bucket_start DESC);

CREATE TABLE IF NOT EXISTS trading_live.corporate_actions (
    corporate_action_id BIGSERIAL PRIMARY KEY,
    market TEXT NOT NULL,
    symbol TEXT NOT NULL,
    action_type TEXT NOT NULL,
    title TEXT,
    cash_amount NUMERIC(20, 8),
    shares_per_stock NUMERIC(20, 8),
    ratio NUMERIC(20, 8),
    as_of TIMESTAMPTZ NOT NULL,
    raw_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (market, symbol, action_type, as_of, title)
);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('trading_live.ohlcv', 'bucket_start', if_not_exists => TRUE);
    END IF;
END $$;
