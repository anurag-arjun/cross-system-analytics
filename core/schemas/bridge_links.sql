-- /core/schemas/bridge_links.sql
-- Cross-chain bridge linking table. Joins bridge_out on source chain
-- to bridge_in on destination chain via link_key.

CREATE TABLE IF NOT EXISTS bridge_links (
  link_key           String,
  link_key_type      LowCardinality(String),  -- 'across_deposit_id', 'layerzero_guid', etc.
  
  src_chain          LowCardinality(String),
  src_block_time     DateTime64(3),
  src_tx_hash        String,
  src_entity_id      String,
  src_event_id       FixedString(32),
  
  dst_chain          LowCardinality(String),
  dst_block_time     DateTime64(3),
  dst_tx_hash        String,
  dst_entity_id      String,
  dst_event_id       FixedString(32),
  
  token              FixedString(20),
  amount             Decimal(76, 0),
  amount_usd         Nullable(Decimal(38, 6)),
  
  link_confidence    Float32 DEFAULT 1.0,
  validated_at       DateTime64(3) DEFAULT now()
) ENGINE = MergeTree
ORDER BY (link_key_type, link_key, src_chain, dst_chain)
PARTITION BY toYYYYMM(src_block_time);
