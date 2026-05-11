-- /core/schemas/pool_registry.sql
-- Uniswap V2/V3 pool registry for token resolution.

CREATE TABLE IF NOT EXISTS nexus.pool_registry (
  pool_address       String,
  chain              LowCardinality(String),
  protocol           LowCardinality(String),
  token0             String,
  token1             String,
  fee                Nullable(UInt32),  -- V3 fee tier in bps (500, 3000, 10000)
  inserted_at        DateTime64(3) DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (chain, pool_address);
