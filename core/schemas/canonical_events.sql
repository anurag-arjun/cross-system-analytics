-- /core/schemas/canonical_events.sql
-- Run this via ClickHouse init or manually.
-- Sort key (entity_id, timestamp) optimises trajectory queries.

CREATE TABLE IF NOT EXISTS canonical_events (
  -- identity (generic, not wallet-specific)
  entity_id          String,
  entity_type        LowCardinality(String),  -- 'wallet', 'user_id', 'email_hash', 'device_id'

  -- event
  event_id           FixedString(32),         -- deterministic hash(source_system, source_event_id)
  event_type         LowCardinality(String),  -- see registry.yaml
  event_category     LowCardinality(String),  -- 'transaction', 'product', 'acquisition', 'lifecycle'
  timestamp          DateTime64(3),

  -- source
  source_system      LowCardinality(String),  -- 'evm_ethereum', 'evm_base', 'ga4', 'posthog'
  source_event_id    String,                  -- raw ID from source, for deduplication
  chain              LowCardinality(String),  -- NULL for Web2 sources
  block_number       Nullable(UInt64),
  block_time         Nullable(DateTime64(3)),
  tx_hash            Nullable(FixedString(32)),
  log_index          Nullable(UInt32),

  -- classification
  protocol           LowCardinality(String),
  venue              LowCardinality(String),   -- pool/market/specific contract

  -- flow (EVM-specific; nullable for Web2 events)
  token_in           Nullable(FixedString(20)),
  token_out          Nullable(FixedString(20)),
  amount_in          Nullable(Decimal(76, 0)), -- raw units, decimals in token table
  amount_out         Nullable(Decimal(76, 0)),
  amount_in_usd      Nullable(Decimal(38, 6)), -- enriched via token_prices ASOF
  amount_out_usd     Nullable(Decimal(38, 6)),

  -- counterparty / routing
  counterparty       Nullable(FixedString(20)),
  aggregator         LowCardinality(String),   -- '1inch','0x','cowswap', else ''

  -- linking (for cross-chain / cross-system stitching)
  link_key           Nullable(String),         -- e.g. depositId, guid, session_id
  link_key_type      Nullable(String),

  -- free-form
  extra              String CODEC(ZSTD(3))     -- JSON, schema-validated per event_type
) ENGINE = MergeTree
ORDER BY (entity_id, timestamp)
PARTITION BY toYYYYMM(timestamp);
