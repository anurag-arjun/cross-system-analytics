-- /core/schemas/token_metadata.sql
-- Static token metadata: symbols, decimals, names per chain.
-- Populated once per token; decimals rarely change.

CREATE TABLE IF NOT EXISTS token_metadata (
  token_address      FixedString(20),
  chain              LowCardinality(String),
  symbol             LowCardinality(String),
  decimals           UInt8,
  name               String,
  inserted_at        DateTime64(3) DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (chain, token_address);
