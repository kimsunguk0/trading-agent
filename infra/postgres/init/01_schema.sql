CREATE SCHEMA IF NOT EXISTS trading_paper;
CREATE SCHEMA IF NOT EXISTS trading_live;

SET search_path TO trading_paper;

CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    environment TEXT NOT NULL,
    cash_balance NUMERIC(20,8) NOT NULL,
    available_cash NUMERIC(20,8) NOT NULL,
    currency TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cash_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL,
    cash_balance NUMERIC(20,8) NOT NULL,
    available_cash NUMERIC(20,8) NOT NULL,
    reserved_cash NUMERIC(20,8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS position_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    quantity NUMERIC(20,8) NOT NULL,
    average_price NUMERIC(20,8) NOT NULL,
    realized_pnl NUMERIC(20,8) NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS instruments (
    symbol TEXT PRIMARY KEY,
    market TEXT NOT NULL,
    name TEXT,
    currency TEXT,
    exchange TEXT,
    tick_size_rule JSONB NOT NULL DEFAULT '{}'::jsonb,
    lot_size NUMERIC(20,8) NOT NULL DEFAULT 1,
    is_tradable BOOLEAN NOT NULL DEFAULT TRUE,
    is_halted BOOLEAN NOT NULL DEFAULT FALSE,
    is_managed BOOLEAN NOT NULL DEFAULT FALSE,
    market_cap NUMERIC(20,0),
    sector TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_intents (
    order_intent_id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE NOT NULL,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity NUMERIC(20,8) NOT NULL,
    limit_price NUMERIC(20,8),
    order_type TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    order_intent_id TEXT NOT NULL REFERENCES order_intents(order_intent_id),
    status TEXT NOT NULL,
    filled_quantity NUMERIC(20,8) NOT NULL,
    total_quantity NUMERIC(20,8) NOT NULL,
    average_fill_price NUMERIC(20,8),
    rejected_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    order_intent_id TEXT NOT NULL,
    quantity NUMERIC(20,8) NOT NULL,
    price NUMERIC(20,8) NOT NULL,
    filled_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_events (
    risk_event_id BIGSERIAL PRIMARY KEY,
    order_intent_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    reason TEXT,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS system_state_log (
    id BIGSERIAL PRIMARY KEY,
    state TEXT NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT,
    target_id TEXT,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trading_controls (
    control_type TEXT NOT NULL,
    target TEXT NOT NULL,
    blocked BOOLEAN NOT NULL DEFAULT TRUE,
    reason TEXT,
    updated_by TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (control_type, target)
);

SET search_path TO trading_live;

CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    environment TEXT NOT NULL,
    cash_balance NUMERIC(20,8) NOT NULL,
    available_cash NUMERIC(20,8) NOT NULL,
    currency TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cash_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL,
    cash_balance NUMERIC(20,8) NOT NULL,
    available_cash NUMERIC(20,8) NOT NULL,
    reserved_cash NUMERIC(20,8) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS position_snapshots (
    snapshot_id BIGSERIAL PRIMARY KEY,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    quantity NUMERIC(20,8) NOT NULL,
    average_price NUMERIC(20,8) NOT NULL,
    realized_pnl NUMERIC(20,8) NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS instruments (
    symbol TEXT PRIMARY KEY,
    market TEXT NOT NULL,
    name TEXT,
    currency TEXT,
    exchange TEXT,
    tick_size_rule JSONB NOT NULL DEFAULT '{}'::jsonb,
    lot_size NUMERIC(20,8) NOT NULL DEFAULT 1,
    is_tradable BOOLEAN NOT NULL DEFAULT TRUE,
    is_halted BOOLEAN NOT NULL DEFAULT FALSE,
    is_managed BOOLEAN NOT NULL DEFAULT FALSE,
    market_cap NUMERIC(20,0),
    sector TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_intents (
    order_intent_id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE NOT NULL,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity NUMERIC(20,8) NOT NULL,
    limit_price NUMERIC(20,8),
    order_type TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    order_intent_id TEXT NOT NULL REFERENCES order_intents(order_intent_id),
    status TEXT NOT NULL,
    filled_quantity NUMERIC(20,8) NOT NULL,
    total_quantity NUMERIC(20,8) NOT NULL,
    average_fill_price NUMERIC(20,8),
    rejected_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    order_intent_id TEXT NOT NULL,
    quantity NUMERIC(20,8) NOT NULL,
    price NUMERIC(20,8) NOT NULL,
    filled_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_events (
    risk_event_id BIGSERIAL PRIMARY KEY,
    order_intent_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    reason TEXT,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS system_state_log (
    id BIGSERIAL PRIMARY KEY,
    state TEXT NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT,
    target_id TEXT,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trading_controls (
    control_type TEXT NOT NULL,
    target TEXT NOT NULL,
    blocked BOOLEAN NOT NULL DEFAULT TRUE,
    reason TEXT,
    updated_by TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (control_type, target)
);
