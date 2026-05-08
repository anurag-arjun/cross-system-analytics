# Nexus Analytics — Facts

**Last reconciled:** 2026-05-08

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
[2026-05-08] Generic ABI decoder framework: per-protocol YAML mapping under `core/adapters/evm/decoders/mappings/{protocol}.yaml` + optional plugin in `plugins.py` replaces hand-written `LogDecoder` subclasses for one-log-to-one-event protocols. Bespoke classes reserved for bridges/aggregators with stateful or multi-log logic.
[2026-05-08] Address-first decoder lookup: `DecoderRegistry.lookup(topic0, address, chain)` tries (chain, address) → protocol → YAML mapping first, then falls back to topic0. Required for shared-topic0 disambiguation (UniV2 vs Sushi vs Pancake).
[2026-05-08] Address registry: Postgres `protocol_contracts` (chain, address, protocol, version, contract_type, source) with composite PK on (chain, address, source). Sources coexist; lookup priority manual > dune > spellbook.
[2026-05-08] Label registry: Postgres `contract_labels` (chain, address, label, category, source). Populated by JOIN of Dune `labels.addresses` against known address universe — never unfiltered.
[2026-05-08] ABI cache: Postgres `protocol_abis` (code_hash PK, abi_json, source). Bytecode-hash dedup means factory-deployed pools share one ABI entry.
[2026-05-08] Bytecode index: Postgres `contract_bytecodes` (chain, address PK, code_hash, is_proxy, implementation_address). EIP-1967 proxies resolved at fetch time so the ABI cache key is the impl's hash, not the proxy's.

## Conventions

[2026-04-23] Every engineering task tagged `[core]` or `[avail]`. No untagged tasks.
[2026-04-23] Weekly architectural guardrails run every Friday: standalone test, fake-integrator test, schema extensibility test.
[2026-04-23] Three product surfaces in priority order: (1) Nexus integrator CS tool, (2) FastBridge marketing analytics, (3) GTM/ICP scoring.
[2026-04-23] Exposure Scorecard is Phase 2 only (Months 7-9).
[2026-04-23] Bridge linking: Base-only bulletproofing in Week 4-5. Arbitrum/Optimism are preview-quality. Stargate/OFT proven on Base only.
[2026-05-06] Bridge decoders use topic0s verified from contract source code (not Spellbook). Spellbook uses per-chain Swap events and static address labels — no cross-chain matching.
[2026-05-06] Test command: `cd core && PYTHONPATH=.. pytest tests/` (from project root; pytest CLI may need --override-ini due to venv issue).
[2026-05-08] BD scope chains: Ethereum, Base, Arbitrum, Optimism, **Polygon**. HyperEVM/MegaETH/Monad out of scope (HyperEVM has broken HyperSync TLS; MegaETH/Monad endpoints work but BD doesn't need them yet).
[2026-05-08] Adding a protocol decoder = YAML mapping under `core/adapters/evm/decoders/mappings/` + optional plugin function in `plugins.py` + entry in `protocol_contracts` (via Dune bootstrap or manual). Bespoke `LogDecoder` only when one log doesn't cleanly map to one canonical event.
[2026-05-08] Address universe primarily sourced from Dune `dex.trades`, `dex_aggregator.trades`, `bridges_evms.{deposits,withdrawals}` via the bootstrap script. Spellbook seeds contribute ~91 rows. Source priority for the JOIN: manual > dune > spellbook.
[2026-05-08] Dune `labels.addresses` only ever queried via JOIN against the known address universe — never `SELECT *` (1.3B rows would blow free-tier budget).

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
[2026-05-08] Spellbook is dbt SQL aggregation on Dune's already-decoded tables, NOT a decoder library. ABIs live in Dune's proprietary indexer. Spellbook seeds give ~91 contract addresses, not 10k. Real address volume must come from Dune Query API.
[2026-05-08] Dune free tier: 2,500 credits/month, 20 credits/MB exported (free) or 2 (Plus). Full registry+labels bootstrap (1-day window, 5 chains) = ~268 credits ≈ 9 runs/month.
[2026-05-08] Dune Execute SQL request body uses field name `sql` (not `query_sql`). Verified 2026-05-08.
[2026-05-08] Dune `labels.addresses` cardinality: ~1.3B rows across 5 chains (974M Base alone). JOIN-filter is mandatory.
[2026-05-08] Dune results endpoint paginates at 10k rows by default. `core.registry.dune.DuneClient.execute_sql` offset-pages automatically; if you write a custom poller, do the same.
[2026-05-08] Dune `bridges_evms.{deposits,withdrawals}` schema differs from `dex.trades`: uses `{deposit_chain|withdrawal_chain, bridge_name, bridge_version, contract_address}` not `{blockchain, project, version, project_contract_address}`.
[2026-05-08] UniV2 `Swap` event has indexed args at non-contiguous positions (sender@0, to@5). Generic decoder uses per-arg `indexed: true` flag, not a single `indexed_count` integer.
[2026-05-08] HyperEVM HyperSync endpoint has broken TLS cert (UnknownIssuer). MegaETH and Monad endpoints DO work — out of scope on need-not-feasibility, not blocked.
[2026-05-08] `api.etherscan.io` NXDOMAIN issue on systemd-resolved with negative caching. Workaround applied to this machine: `/etc/hosts` entry `217.79.243.34 api.etherscan.io`. If DNS regresses, re-add or fix resolved.conf to use 1.1.1.1.
[2026-05-08] Etherscan V2 unified API: one `ETHERSCAN_API_KEY` works across Ethereum, Base, Arbitrum, Optimism, Polygon at `api.etherscan.io/v2/api` with `chainid` query param. Free tier ~5 req/sec.

## Dependencies & Tooling

[2026-05-08] HyperSync URLs (verified live with token): eth.hypersync.xyz, base.hypersync.xyz, arbitrum.hypersync.xyz, optimism.hypersync.xyz, **polygon.hypersync.xyz**.
[2026-04-23] ClickHouse + Postgres via Docker Compose. Dagster for orchestration. Observable Framework for developer-facing analytics dashboards.
[2026-04-23] Docker Compose stack includes: ClickHouse, Postgres, Dagster, Observable Framework.
[2026-04-23] `tk` CLI for ticket tracking (stored in `.tickets/`).
[2026-05-06] CoinGecko API key: CG-rRAy9PuJ2yowpLftyDTk9PGJ (free tier, 10K calls/month).
[2026-05-06] DexScreener API: free, no key, used as fallback when COINGECKO_API_KEY not set.
[2026-05-06] PriceResolver: caches prices from `token_prices` + `token_metadata` ClickHouse tables.
[2026-05-06] Bridge research: `docs/BRIDGE_RESEARCH.md` (verified against Spellbook + on-chain).
[2026-05-06] Environ vars: HYPERSYNC_TOKEN, COINGECKO_API_KEY (optional), in `.env`.
[2026-05-08] Env vars: ETHERSCAN_API_KEY (Etherscan V2), DUNE_API_KEY (Dune Query API), PROTOCOL_CONTRACTS_DSN (optional Postgres for registry; default `postgresql://nexus:nexus@localhost:5434/nexus_ops`), SPELLBOOK_PATH (defaults to sibling clone).
[2026-05-08] Spellbook clone at `/home/lighto/code/avail-explorations/spellbook` (shallow ~249 MB).
[2026-05-08] Make targets: `make import-spellbook`, `make import-dune`, `make fetch-abis`.

## Preferences

[2026-04-23] IP agreement with Avail leadership is prerequisite for all production code.
[2026-04-23] No open-source launch before Month 12. Build the product first; open-source second.
[2026-05-08] Verify framing claims with real API calls before scoping numbers in tickets/plans. The spellbook "10k+ rows" estimate was wrong by ~300×; an audit-pass mid-session caught it before further damage. Apply this pattern when sizing new external-data work.

## Superseded

## Stale
