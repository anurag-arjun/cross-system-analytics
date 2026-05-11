-- /core/schemas/token_prices.sql
-- Time-series token prices for USD enrichment.
-- Updated hourly; ASOF JOIN against canonical_events.timestamp.

CREATE TABLE IF NOT EXISTS nexus.token_prices (
  token_address      String,
  chain              LowCardinality(String),
  timestamp          DateTime64(3),
  price_usd          Float64,
  source             LowCardinality(String),
  volume_24h_usd     Nullable(Float64),
  inserted_at        DateTime64(3) DEFAULT now()
) ENGINE = MergeTree
ORDER BY (chain, token_address, timestamp)
PARTITION BY toYYYYMM(timestamp);
