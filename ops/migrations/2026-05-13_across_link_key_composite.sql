-- 2026-05-13: Across link_key composite (chain_id, deposit_id)
--
-- Background:
--   Across uses per-SpokePool depositId counters — arbitrum #5000 ≠
--   ethereum #5000. The original decoder set link_key=str(deposit_id),
--   so cross-chain matches were coincidence (and some real ones were
--   missed, others were false positives).
--
-- Code fix:
--   core/adapters/evm/__init__.py:_composite_link_key — live ingest now
--   prefixes Across link_keys with chain_id on both sides.
--
-- This script rewrites EXISTING rows in canonical_events. Idempotent
-- via `position(link_key, ':') = 0` guard.
--
-- Run:
--   docker exec -i nexus-ch clickhouse-client \
--     --database=nexus --query="$(cat 2026-05-13_across_link_key_composite.sql)"
--
-- Or set mutations_sync=2 to block until done.

SET mutations_sync = 2;

-- Mutation 1: bridge_out — prefix with chain_id derived from `chain` name.
ALTER TABLE nexus.canonical_events UPDATE
  link_key = concat(
    multiIf(
      chain = 'ethereum', '1',
      chain = 'optimism', '10',
      chain = 'arbitrum', '42161',
      chain = 'base',     '8453',
      chain = 'polygon',  '137',
      chain
    ),
    ':', assumeNotNull(link_key)
  )
WHERE event_type      = 'bridge_out'
  AND link_key_type   = 'across_deposit_id'
  AND link_key IS NOT NULL
  AND position(link_key, ':') = 0;

-- Mutation 2: bridge_in — prefix with origin_chain_id from JSON extra.
-- Most rows parse fine; the JSON path covers the bulk.
ALTER TABLE nexus.canonical_events UPDATE
  link_key = concat(
    toString(JSONExtractInt(extra, 'origin_chain_id')),
    ':', assumeNotNull(link_key)
  )
WHERE event_type      = 'bridge_in'
  AND link_key_type   = 'across_deposit_id'
  AND link_key IS NOT NULL
  AND position(link_key, ':') = 0
  AND JSONExtractInt(extra, 'origin_chain_id') > 0;

-- Mutation 3: bridge_in — regex fallback for V3-hash deposit_ids that
-- overflow CH's JSON int parser (the huge uint256 is in the same object
-- as origin_chain_id and breaks the parse).
ALTER TABLE nexus.canonical_events UPDATE
  link_key = concat(
    extract(extra, '"origin_chain_id"\s*:\s*(\d+)'),
    ':', assumeNotNull(link_key)
  )
WHERE event_type      = 'bridge_in'
  AND link_key_type   = 'across_deposit_id'
  AND link_key IS NOT NULL
  AND position(link_key, ':') = 0
  AND extract(extra, '"origin_chain_id"\s*:\s*(\d+)') != '';
