-- Migration: switch canonical_events, canonical_logs, bridge_links from
-- MergeTree to ReplacingMergeTree. Removes the need for ClickHouseSink's
-- pre-insert dedup SELECT; the engine collapses duplicates at merge time.
--
-- ORDER BY changes per table:
--   canonical_events: (entity_id, timestamp) -> (entity_id, timestamp, event_id)
--   canonical_logs:   (block_number, log_index) -> (chain, block_number, tx_hash, log_index)
--   bridge_links:     (link_key_type, link_key, src_chain, dst_chain)
--                  -> (..., src_event_id, dst_event_id)
--
-- canonical_logs also changes PARTITION BY toYYYYMM(inserted_at) -> chain,
-- because inserted_at-based partitioning prevents merge-time dedup
-- across re-ingestion runs.
--
-- Execution model per table:
--   1. CREATE TABLE _new with the new engine
--   2. INSERT INTO _new SELECT * FROM original
--   3. EXCHANGE TABLES original AND _new  (atomic on Atomic DB engine)
--   4. DROP TABLE _new  (now holds the old data)
--   5. OPTIMIZE TABLE original FINAL DEDUPLICATE  (collapse known dupes)
--
-- Run with --multiquery. Comments after `--` are stripped by clickhouse-client.

-- =========================================================================
-- canonical_events
-- =========================================================================

CREATE TABLE IF NOT EXISTS nexus.canonical_events_new (
  entity_id          String,
  entity_type        LowCardinality(String),

  event_id           String,
  event_type         LowCardinality(String),
  event_category     LowCardinality(String),
  timestamp          DateTime64(3),

  source_system      LowCardinality(String),
  source_event_id    String,
  chain              LowCardinality(String),
  block_number       Nullable(UInt64),
  block_time         Nullable(DateTime64(3)),
  tx_hash            Nullable(String),
  log_index          Nullable(UInt32),

  protocol           LowCardinality(String),
  venue              String,

  token_in           Nullable(String),
  token_out          Nullable(String),
  amount_in          Nullable(String),
  amount_out         Nullable(String),
  amount_in_usd      Nullable(Float64),
  amount_out_usd     Nullable(Float64),

  counterparty       Nullable(String),
  aggregator         LowCardinality(String),

  link_key           Nullable(String),
  link_key_type      Nullable(String),

  extra              String CODEC(ZSTD(3))
) ENGINE = ReplacingMergeTree
ORDER BY (entity_id, timestamp, event_id)
PARTITION BY toYYYYMM(timestamp);

INSERT INTO nexus.canonical_events_new SELECT * FROM nexus.canonical_events;

EXCHANGE TABLES nexus.canonical_events AND nexus.canonical_events_new;

DROP TABLE nexus.canonical_events_new;

OPTIMIZE TABLE nexus.canonical_events FINAL DEDUPLICATE;

-- =========================================================================
-- canonical_logs
-- =========================================================================

CREATE TABLE IF NOT EXISTS nexus.canonical_logs_new (
  source_system      LowCardinality(String),
  chain              LowCardinality(String),
  block_number       UInt64,
  block_time         Nullable(DateTime64(3)),
  tx_hash            FixedString(66),
  log_index          UInt32,
  address            FixedString(42),
  topic0             Nullable(FixedString(66)),
  topic1             Nullable(FixedString(66)),
  topic2             Nullable(FixedString(66)),
  topic3             Nullable(FixedString(66)),
  data               String,
  decoded            UInt8 DEFAULT 0,
  decoder_version    UInt32 DEFAULT 0,
  inserted_at        DateTime64(3) DEFAULT now()
) ENGINE = ReplacingMergeTree
ORDER BY (chain, block_number, tx_hash, log_index)
PARTITION BY chain;

INSERT INTO nexus.canonical_logs_new SELECT * FROM nexus.canonical_logs;

EXCHANGE TABLES nexus.canonical_logs AND nexus.canonical_logs_new;

DROP TABLE nexus.canonical_logs_new;

OPTIMIZE TABLE nexus.canonical_logs FINAL DEDUPLICATE;

-- =========================================================================
-- bridge_links
-- =========================================================================

CREATE TABLE IF NOT EXISTS nexus.bridge_links_new (
  link_key           String,
  link_key_type      LowCardinality(String),

  src_chain          LowCardinality(String),
  src_block_time     DateTime64(3),
  src_tx_hash        String,
  src_entity_id      String,
  src_event_id       String,

  dst_chain          LowCardinality(String),
  dst_block_time     DateTime64(3),
  dst_tx_hash        String,
  dst_entity_id      String,
  dst_event_id       String,

  token              String,
  amount             String,
  amount_usd         Nullable(Float64),

  link_confidence    Float32 DEFAULT 1.0,
  validated_at       DateTime64(3) DEFAULT now()
) ENGINE = ReplacingMergeTree
ORDER BY (link_key_type, link_key, src_chain, dst_chain, src_event_id, dst_event_id)
PARTITION BY toYYYYMM(src_block_time);

INSERT INTO nexus.bridge_links_new SELECT * FROM nexus.bridge_links;

EXCHANGE TABLES nexus.bridge_links AND nexus.bridge_links_new;

DROP TABLE nexus.bridge_links_new;

OPTIMIZE TABLE nexus.bridge_links FINAL DEDUPLICATE;
