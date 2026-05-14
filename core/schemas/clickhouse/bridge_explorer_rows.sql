CREATE TABLE IF NOT EXISTS nexus.bridge_explorer_rows (
  window_start       DateTime64(3),
  window_end         DateTime64(3),
  row_type           LowCardinality(String),
  link_key           Nullable(String),
  link_key_type      Nullable(String),
  bridge             LowCardinality(String),

  src_chain          Nullable(String),
  src_block_time     Nullable(DateTime64(3)),
  src_tx_hash        Nullable(String),
  src_entity_id      Nullable(String),
  src_event_id       Nullable(String),

  dst_chain          Nullable(String),
  dst_block_time     Nullable(DateTime64(3)),
  dst_tx_hash        Nullable(String),
  dst_entity_id      Nullable(String),
  dst_event_id       Nullable(String),

  src_token          Nullable(String),
  src_amount         Nullable(String),
  src_amount_usd     Nullable(Float64),
  dst_token          Nullable(String),
  dst_amount         Nullable(String),
  dst_amount_usd     Nullable(Float64),

  latency_seconds    Nullable(Int32),
  dst_chain_id_hint  String,
  src_chain_id_hint  String,

  status             LowCardinality(String),
  tags               Array(String),
  status_reason      String,
  sort_time          DateTime64(3),
  materialized_at    DateTime64(3) DEFAULT now()
) ENGINE = ReplacingMergeTree(materialized_at)
ORDER BY (window_start, window_end, row_type, src_event_id, dst_event_id, link_key, link_key_type)
PARTITION BY toYYYYMM(window_start);
