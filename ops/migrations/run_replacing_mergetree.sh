#!/usr/bin/env bash
# Idempotent runner for 2026-05-13_replacing_mergetree.sql.
#
# Migrates canonical_events / canonical_logs / bridge_links from MergeTree
# to ReplacingMergeTree, one table at a time, with row-count validation
# between INSERT and EXCHANGE. Aborts on any mismatch so a partial INSERT
# can never atomic-swap in a smaller dataset.
#
# Usage:
#   CH_CONTAINER=nexus-analytics-clickhouse-1 ./run_replacing_mergetree.sh        # dev
#   ssh shieldtx-vps "bash -s" < run_replacing_mergetree.sh                       # prod (mount via stdin)
#
# Env:
#   CH_CONTAINER     docker container name (default: nexus-analytics-clickhouse-1)
#   CH_USER          (default: default)
#   CH_PASSWORD      (default: nexus)
#   CH_DATABASE      (default: nexus)
#   SKIP_OPTIMIZE    set to 1 to skip OPTIMIZE FINAL DEDUPLICATE (faster, dupes
#                    collapse on natural merges instead)

set -euo pipefail

: "${CH_CONTAINER:=nexus-analytics-clickhouse-1}"
: "${CH_USER:=default}"
: "${CH_PASSWORD:=nexus}"
: "${CH_DATABASE:=nexus}"
: "${SKIP_OPTIMIZE:=0}"

ch() {
  docker exec "$CH_CONTAINER" clickhouse-client \
    -u "$CH_USER" --password "$CH_PASSWORD" --database "$CH_DATABASE" \
    --query "$1"
}

ch_multi() {
  docker exec -i "$CH_CONTAINER" clickhouse-client \
    -u "$CH_USER" --password "$CH_PASSWORD" --database "$CH_DATABASE" \
    --multiquery
}

count() {
  ch "SELECT count() FROM ${CH_DATABASE}.$1"
}

engine_of() {
  ch "SELECT engine FROM system.tables WHERE database = '${CH_DATABASE}' AND name = '$1'"
}

migrate_table() {
  local table="$1"
  local order_by="$2"
  local partition_by="$3"
  local schema_body="$4"

  echo "================================================================"
  echo "MIGRATING: ${table}"
  echo "  ORDER BY:     ${order_by}"
  echo "  PARTITION BY: ${partition_by}"
  echo "================================================================"

  local cur_engine
  cur_engine=$(engine_of "${table}")
  if [[ "${cur_engine}" == "ReplacingMergeTree" ]]; then
    echo "  SKIP — ${table} is already ReplacingMergeTree"
    return 0
  fi

  local before
  before=$(count "${table}")
  echo "  rows before:  ${before}"

  # uniqExact on the ORDER BY tuple = number of distinct rows that the new
  # ReplacingMergeTree will preserve. This count is conserved across the
  # engine change — if it doesn't match in _new, data was lost.
  local before_unique
  before_unique=$(ch "SELECT uniqExact${order_by} FROM ${CH_DATABASE}.${table}")
  echo "  unique tuples: ${before_unique}  (expected dupes to collapse: $((before - before_unique)))"

  # Clean up any stale _new from a previous aborted run.
  ch "DROP TABLE IF EXISTS ${CH_DATABASE}.${table}_new" >/dev/null

  echo "  CREATE ${table}_new ..."
  ch_multi <<SQL
CREATE TABLE ${CH_DATABASE}.${table}_new (
${schema_body}
) ENGINE = ReplacingMergeTree
ORDER BY ${order_by}
PARTITION BY ${partition_by};
SQL

  echo "  INSERT INTO ${table}_new SELECT * FROM ${table} ..."
  local t0=$(date +%s)
  ch "INSERT INTO ${CH_DATABASE}.${table}_new SELECT * FROM ${CH_DATABASE}.${table}"
  local t1=$(date +%s)
  echo "    ($((t1-t0))s)"

  local new_rows new_unique
  new_rows=$(count "${table}_new")
  new_unique=$(ch "SELECT uniqExact${order_by} FROM ${CH_DATABASE}.${table}_new")
  echo "  rows in _new:    ${new_rows}  (post-implicit-merge)"
  echo "  unique in _new:  ${new_unique}"

  if [[ "${new_unique}" != "${before_unique}" ]]; then
    echo "  ABORT — unique-tuple mismatch (old=${before_unique}, new=${new_unique})"
    echo "          ${table}_new retained for inspection. ${table} untouched."
    exit 1
  fi

  echo "  EXCHANGE TABLES ${table} AND ${table}_new ..."
  ch "EXCHANGE TABLES ${CH_DATABASE}.${table} AND ${CH_DATABASE}.${table}_new"

  echo "  DROP TABLE ${table}_new (now holds the old data) ..."
  ch "DROP TABLE ${CH_DATABASE}.${table}_new"

  local after
  after=$(count "${table}")
  echo "  rows after (pre-optimize):  ${after}"

  if [[ "${SKIP_OPTIMIZE}" == "0" ]]; then
    echo "  OPTIMIZE TABLE ${table} FINAL DEDUPLICATE ..."
    local t2=$(date +%s)
    ch "OPTIMIZE TABLE ${CH_DATABASE}.${table} FINAL DEDUPLICATE"
    local t3=$(date +%s)
    echo "    ($((t3-t2))s)"
    local optimized
    optimized=$(count "${table}")
    echo "  rows after optimize:        ${optimized}  (delta: $((after - optimized)) dupes collapsed)"
  fi

  local final_engine
  final_engine=$(engine_of "${table}")
  echo "  engine: ${final_engine}  ✓"
}

# Tables in dependency-free order. canonical_events first (most queries),
# then canonical_logs (large but rarely read), then bridge_links (tiny).

migrate_table "canonical_events" \
  "(entity_id, timestamp, event_id)" \
  "toYYYYMM(timestamp)" \
"  entity_id          String,
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
  extra              String CODEC(ZSTD(3))"

migrate_table "canonical_logs" \
  "(chain, block_number, tx_hash, log_index)" \
  "chain" \
"  source_system      LowCardinality(String),
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
  inserted_at        DateTime64(3) DEFAULT now()"

migrate_table "bridge_links" \
  "(link_key_type, link_key, src_chain, dst_chain, src_event_id, dst_event_id)" \
  "toYYYYMM(src_block_time)" \
"  link_key           String,
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
  validated_at       DateTime64(3) DEFAULT now()"

echo "================================================================"
echo "ALL TABLES MIGRATED."
echo "================================================================"
