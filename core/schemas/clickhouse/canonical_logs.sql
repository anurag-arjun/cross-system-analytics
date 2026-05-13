CREATE TABLE IF NOT EXISTS nexus.canonical_logs (
  source_system      LowCardinality(String),  -- 'evm_base', 'evm_ethereum', etc.
  chain              LowCardinality(String),
  block_number       UInt64,
  block_time         Nullable(DateTime64(3)),
  tx_hash            FixedString(66),
  log_index          UInt32,
  address            FixedString(42),          -- contract address
  topic0             Nullable(FixedString(66)),
  topic1             Nullable(FixedString(66)),
  topic2             Nullable(FixedString(66)),
  topic3             Nullable(FixedString(66)),
  data               String,                   -- hex event data
  decoded            UInt8 DEFAULT 0,          -- 0 = undecoded, 1 = decoded
  decoder_version    UInt32 DEFAULT 0,         -- registry version that decoded this
  inserted_at        DateTime64(3) DEFAULT now()
) ENGINE = ReplacingMergeTree
ORDER BY (chain, block_number, tx_hash, log_index)
PARTITION BY chain;
-- Dedup happens at merge time on the ORDER BY tuple. (chain, block_number,
-- tx_hash, log_index) is the natural key for an EVM log. Partition by
-- chain (not by month-of-inserted_at) so re-ingesting an old log lands in
-- the same partition as the original — without this, merges can't see
-- the duplicate and dedup never fires.
