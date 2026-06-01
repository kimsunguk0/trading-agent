CREATE TABLE IF NOT EXISTS trading_paper.slippage_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_intent_id TEXT NOT NULL,
    broker_order_id TEXT NOT NULL,
    symbol_market TEXT,
    symbol_code TEXT,
    strategy_id TEXT,
    intended_price NUMERIC(20,8) NOT NULL,
    filled_price NUMERIC(20,8) NOT NULL,
    slippage_pct NUMERIC(10,6) NOT NULL,
    side TEXT,
    quantity NUMERIC(20,8) NOT NULL,
    filled_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trading_paper.reconciliation_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol_market TEXT,
    symbol_code TEXT NOT NULL,
    internal_qty NUMERIC(20,8) NOT NULL,
    broker_qty NUMERIC(20,8) NOT NULL,
    diff_qty NUMERIC(20,8) NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('ok', 'warning', 'critical')),
    action_taken TEXT,
    reconciled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trading_live.slippage_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_intent_id TEXT NOT NULL,
    broker_order_id TEXT NOT NULL,
    symbol_market TEXT,
    symbol_code TEXT,
    strategy_id TEXT,
    intended_price NUMERIC(20,8) NOT NULL,
    filled_price NUMERIC(20,8) NOT NULL,
    slippage_pct NUMERIC(10,6) NOT NULL,
    side TEXT,
    quantity NUMERIC(20,8) NOT NULL,
    filled_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trading_live.reconciliation_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol_market TEXT,
    symbol_code TEXT NOT NULL,
    internal_qty NUMERIC(20,8) NOT NULL,
    broker_qty NUMERIC(20,8) NOT NULL,
    diff_qty NUMERIC(20,8) NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('ok', 'warning', 'critical')),
    action_taken TEXT,
    reconciled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
