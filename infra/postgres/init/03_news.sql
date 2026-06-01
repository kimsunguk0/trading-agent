CREATE TABLE IF NOT EXISTS trading_paper.normalized_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    symbol_market TEXT,
    symbol_code TEXT,
    sentiment_score NUMERIC(20,8),
    catalyst_score NUMERIC(20,8),
    risk_score NUMERIC(20,8),
    source_quality NUMERIC(20,8),
    event_time TIMESTAMPTZ NOT NULL,
    evidence_json JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trading_paper.agent_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name TEXT NOT NULL,
    model_provider TEXT,
    model_name TEXT,
    prompt_version TEXT,
    prompt_hash TEXT,
    input_refs JSONB,
    output_json JSONB,
    tokens_in INT,
    tokens_out INT,
    cost_usd NUMERIC(10,6),
    cache_hit BOOLEAN DEFAULT FALSE,
    latency_ms INT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trading_paper.news_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source TEXT NOT NULL,
    url TEXT UNIQUE,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    body_hash TEXT UNIQUE,
    published_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    raw_json JSONB
);

CREATE TABLE IF NOT EXISTS trading_live.normalized_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    symbol_market TEXT,
    symbol_code TEXT,
    sentiment_score NUMERIC(20,8),
    catalyst_score NUMERIC(20,8),
    risk_score NUMERIC(20,8),
    source_quality NUMERIC(20,8),
    event_time TIMESTAMPTZ NOT NULL,
    evidence_json JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trading_live.agent_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name TEXT NOT NULL,
    model_provider TEXT,
    model_name TEXT,
    prompt_version TEXT,
    prompt_hash TEXT,
    input_refs JSONB,
    output_json JSONB,
    tokens_in INT,
    tokens_out INT,
    cost_usd NUMERIC(10,6),
    cache_hit BOOLEAN DEFAULT FALSE,
    latency_ms INT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
