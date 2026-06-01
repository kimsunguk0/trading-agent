CREATE TABLE IF NOT EXISTS trading_paper.fx_rates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  base_currency TEXT NOT NULL,
  quote_currency TEXT NOT NULL,
  rate NUMERIC(20,8) NOT NULL,
  source TEXT,
  fetched_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS trading_paper.earnings_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  symbol_market TEXT NOT NULL,
  symbol_code TEXT NOT NULL,
  reported_at TIMESTAMPTZ NOT NULL,
  eps_actual NUMERIC(20,8),
  eps_estimate NUMERIC(20,8),
  eps_surprise_pct NUMERIC(10,6),
  revenue_actual NUMERIC(20,8),
  revenue_estimate NUMERIC(20,8),
  revenue_surprise_pct NUMERIC(10,6),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trading_live.fx_rates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  base_currency TEXT NOT NULL,
  quote_currency TEXT NOT NULL,
  rate NUMERIC(20,8) NOT NULL,
  source TEXT,
  fetched_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS trading_live.earnings_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  symbol_market TEXT NOT NULL,
  symbol_code TEXT NOT NULL,
  reported_at TIMESTAMPTZ NOT NULL,
  eps_actual NUMERIC(20,8),
  eps_estimate NUMERIC(20,8),
  eps_surprise_pct NUMERIC(10,6),
  revenue_actual NUMERIC(20,8),
  revenue_estimate NUMERIC(20,8),
  revenue_surprise_pct NUMERIC(10,6),
  created_at TIMESTAMPTZ DEFAULT NOW()
);
