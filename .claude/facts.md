# Nexus Analytics — Facts

**Last reconciled:** 2026-05-06

## Architecture

[2026-04-23] Monorepo split: `/core` (MIT-destined), `/avail` (proprietary), `/commercial` (future enterprise tier).
[2026-04-23] Canonical event schema uses `entity_id` + `entity_type` (not wallet-specific) for horizontal compatibility.
[2026-04-23] Sort key `(entity_id, timestamp)` optimizes trajectory queries.
[2026-04-23] Schema registry in `/core/schemas/registry.yaml` — adding event types requires registry entry + adapter + test.
[2026-04-23] Trajectory primitive `trajectory(entity_id, anchor, window_before, window_after)` is the core query. Target: <2s for 30-day window.
[2026-04-23] Identity graph uses `entity_relationships` table (not flat `wallet_identity`). Supports multi-hop resolution with confidence scores.
[2026-04-23] No Iceberg in v1. ClickHouse MergeTree is primary storage. Revisit when data exceeds 1TB or multi-engine querying is needed.
[2026-05-06] ClickHouse database is `nexus` (not `default`). All SinkConfig, Dagster resources, and enrichment configs use `nexus`.
[2026-05-06] Bridge matching uses Postgres `pending_bridge_outs` table with retry scheduling (ADR-001 implemented).
[2026-05-06] OP Stack L2→L1 withdrawals use withdrawalHash for precise matching (ADR-002 implemented).
[2026-05-06] Aggregator dedup: swap → swap_internal when aggregator event exists in same tx.
[2026-05-06] Stablecoin override at ingestion: 22 tokens across all 4 chains always resolve to $1.00.
[2026-05-06] USD at ingestion via PriceResolver: CoinGecko primary, DexScreener zero-auth fallback.
[2026-05-06] Dedup queries chunked to 1000 IDs to avoid ClickHouse HTTP field size limit.

## Conventions

[2026-04-23] Every engineering task tagged `[core]` or `[avail]`. No untagged tasks.
[2026-04-23] Weekly architectural guardrails run every Friday: standalone test, fake-integrator test, schema extensibility test.
[2026-04-23] Three product surfaces in priority order: (1) Nexus integrator CS tool, (2) FastBridge marketing analytics, (3) GTM/ICP scoring.
[2026-04-23] Exposure Scorecard is Phase 2 only (Months 7-9).
[2026-04-23] Bridge linking: Base-only bulletproofing in Week 4-5. Arbitrum/Optimism are preview-quality. Stargate/OFT proven on Base only.
[2026-05-06] Bridge decoders use topic0s verified from contract source code (not Spellbook). Spellbook uses per-chain Swap events and static address labels — no cross-chain matching.
[2026-05-06] Test command: `cd core && PYTHONPATH=.. pytest tests/` (from project root; pytest CLI may need --override-ini due to venv issue).

## Gotchas

[2026-04-23] HyperSync free tier is primary data source. Pocket Network public RPC is free fallback. cryo is tertiary.
[2026-04-23] No AWS S3 public datasets for Base/Arbitrum/Optimism. BigQuery only has Ethereum.
[2026-05-06] HyperSync URL: `eth.hypersync.xyz` for Ethereum (NOT `ethereum.hypersync.xyz`).
[2026-05-06] Across V3FundsDeposited/FilledV3Relay are LEGACY unused events. Active events: FundsDeposited/FilledRelay with bytes32 address types.
[2026-05-06] Across SpokePool address on Base: `0x09aea4...bec64` (NOT `0x09aea4...8B8EF6`).
[2026-05-06] Stargate ReceiveFromChain topic0 is WRONG — 0 events on any chain. Bridge_in is LayerZero PacketDelivered.
[2026-05-06] LayerZero V1 IDs (Spellbook: 101=eth, 184=base) differ from V2 EIDs (30101=eth, 30184=base). Stargate SendToChain uses EVM chain IDs, ReceiveFromChain uses V2 EIDs.
[2026-05-06] ClickHouse `canoical_events` dedup: chunk queries at 1000 IDs max to avoid "Field value too long" HTTP error.
[2026-05-06] asyncio.run() crashes inside async contexts — use `_run_coro()` helper with thread-pool fallback.
[2026-05-06] ClickHouse HTTP auth may fail on container restart — wait 5-10s for init scripts to complete.

## Dependencies & Tooling

[2026-04-23] HyperSync URLs: eth.hypersync.xyz, base.hypersync.xyz, arbitrum.hypersync.xyz, optimism.hypersync.xyz
[2026-04-23] ClickHouse + Postgres via Docker Compose. Dagster for orchestration. Observable Framework for developer-facing analytics dashboards.
[2026-04-23] Docker Compose stack includes: ClickHouse, Postgres, Dagster, Observable Framework.
[2026-04-23] `tk` CLI for ticket tracking (stored in `.tickets/`).
[2026-05-06] CoinGecko API key: CG-rRAy9PuJ2yowpLftyDTk9PGJ (free tier, 10K calls/month).
[2026-05-06] DexScreener API: free, no key, used as fallback when COINGECKO_API_KEY not set.
[2026-05-06] PriceResolver: caches prices from `token_prices` + `token_metadata` ClickHouse tables.
[2026-05-06] Bridge research: `docs/BRIDGE_RESEARCH.md` (verified against Spellbook + on-chain).
[2026-05-06] Environ vars: HYPERSYNC_TOKEN, COINGECKO_API_KEY (optional), in `.env`.

## Preferences

[2026-04-23] IP agreement with Avail leadership is prerequisite for all production code.
[2026-04-23] No open-source launch before Month 12. Build the product first; open-source second.

## Superseded

## Stale
