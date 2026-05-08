# Journal

## 2026-05-08 — BD decoder foundation: framework + registry + Dune bootstrap + ABI fetcher

Worked on: Re-scoped BD chains (Polygon in; HyperEVM/MegaETH/Monad out), audited my
framing claims via real API calls, shipped 4 P1 foundation tickets for protocol-decoder
coverage at scale.

Decisions:
  - **Generic ABI-driven decoders**: YAML mapping per protocol (~15 lines + optional
    plugin) replaces hand-written `LogDecoder` classes. Bespoke kept only for
    bridges/aggregators with stateful or multi-log logic.
  - **Address-first registry lookup**: `registry.lookup(topic0, address, chain)` tries
    chain+address → protocol → YAML mapping first; topic0 fallback for bespoke. Fixes
    shared-topic0 disambiguation across DEX forks.
  - **Dune is the address universe, not spellbook**: spellbook is dbt SQL on Dune's
    already-decoded tables — NOT a decoder library. Real volume from Dune Query API:
    27k contracts + 163k labels per 1-day pull, ~268 credits ≈ 9 bootstraps/month on
    free tier. Spellbook seeds gave 91 rows total.
  - **Audit before scoping**: real API probes caught my own spellbook 10k-rows estimate
    before it caused real damage. New methodology: verify framing claims before sizing.
  - **ABI cache dedup**: bytecode-hash + EIP-1967 proxy resolution = 2× dedup ratio in
    real run; factory-deployed pools share one impl ABI.

Tickets closed: na-8490 (framework), na-9tmq (registry+spellbook importer), na-7p8s
(Dune bootstrap), na-iz3g (Etherscan ABI fetcher).
Cleanup: archived 6 superseded planning docs + 4 stale dagster temp dirs; fresh-start
refiled 26 BD-coverage tickets (85 archived).

Files: core/registry/ (10 new modules), 4 new schemas (protocol_contracts,
contract_labels, protocol_abis, contract_bytecodes), generic decoder + plugins.py +
mappings/, ops/import_{spellbook,dune}_contracts.py + ops/fetch_abis.py, ~50 tests.

Open threads:
  - na-neal Polygon ingestion (30 min, ready)
  - 14 protocol mapping tickets (Aave V3, Lido, Morpho, GMX V2, etc.) ready
  - na-k7h7 new bridges (CCTP/Wormhole/Mayan/etc.) ready
  - Persistent Postgres-backed registry run deferred (Postgres not running)
  - /etc/hosts workaround applied for api.etherscan.io NXDOMAIN issue

## 2026-05-06 — Pipeline stabilization, bridge infrastructure, USD pricing

Worked on: Restarted Docker stack (old data intact), fixed HyperSync integration bugs,
built complete bridge decoder + matching infrastructure across all 3 families (Intent,
Message-Passing, Canonical), added aggregator decoders + dedup, stablecoin registry,
USD-at-ingestion pricing.

Decisions:
  - **Across decoder topic0s were WRONG**: V3FundsDeposited/FilledV3Relay are LEGACY
    ABI migration stubs. Active events are FundsDeposited/FilledRelay with bytes32
    address types. Also SpokePool address was wrong (0x09aea4...bec64 not ...8B8EF6).
  - **Stargate ReceiveFromChain deprecated**: 0 events on any chain. Real bridge_in
    is LayerZero PacketDelivered. Added EID→chain normalization (chain_mapping.py).
  - **Deferred dedup to flush**: 1000x faster ingestion (5,800 ev/s vs 60 ev/s).
    Chunked queries at 1000 IDs to avoid ClickHouse HTTP field limit.
  - **ClickHouse database**: changed from 'default' to 'nexus' across all configs
    (SinkConfig, Dagster, enrichment, demo scripts).
  - **CoinGecko primary, DexScreener fallback**: zero-auth price lookups for USD
    at ingestion via PriceResolver. 29.5% coverage (up from 0%).
  - **OP Stack precise matching (ADR-002)**: MessagePassed on L2 (bridge_out)
    + WithdrawalProven/Finalized on L1 (bridge_in), matched on withdrawalHash.
  - **Bridge research**: cloned Spellbook, cross-referenced all topic0s. Spellbook
    uses per-chain Swap events + static labels, not cross-chain matching.

Tickets closed (12):
  na-5hcn (Postgres pending table), na-5p48 (bridge matching), na-ma17 (OP Stack
  bridge_in decoders), na-snr2 (ingestion dedup), na-oktl/na-75dq/na-gk38 (aggregator
  decoders), na-5dey (aggregator dedup), na-7qle (Stargate endpoint fix),
  na-bugc (stablecoin override), na-dml4 (OP Stack precise matching),
  na-sm8p (USD at ingestion)

Commits: 13. Tests: 157 passing.

Open threads:
  - na-ffe3: Omnichain wallet timeline query (next priority)
  - na-d5gu: Automate hourly CoinGecko price refresh
  - na-xk8z: Arbitrum canonical bridge decoders
  - Need to populate token_metadata for better USD coverage

## 2026-04-23 — Full project foundation: planning, monorepo, data architecture, tickets

Worked on: Analyzed two legacy planning documents, reconciled them into unified product and engineering plans, scaffolded the full monorepo, researched data sources, and created the project tracking system.

Decisions:
  - **Product framing**: The platform is "analytics for cross-system journeys" not "crypto data pipeline". Crypto is the first market, not the only market.
  - **Entity-centric schema**: Replaced wallet-centric `user_address` with generic `entity_id` + `entity_type` so GA4 sessions and EVM swaps coexist in the same table.
  - **Monorepo split**: `/core` (MIT-destined open-source), `/avail` (proprietary Avail logic), `/commercial` (future enterprise tier). Enforced via task tagging `[core]`/`[avail]`.
  - **FastBridge as primary Week 5 deliverable**: Added unified funnel view (GA4 → bridge → swap) as the "holy shit" demo that validates the horizontal thesis. Was completely missing from original plan.
  - **Deferred Exposure Scorecard**: Moved from Week 7-12 to Phase 2 (Months 7-9). Don't build until CS tool is in daily use.
  - **Deferred Twenty CRM**: Use Postgres + Observable + Tooljet for Week 6. Twenty is a Month 2+ nice-to-have.
  - **HyperSync as primary data source**: Switched from cryo + paid RPC ($100-250) to HyperSync free tier ($0) for all 4 chains. 10-100x faster.
  - **No Iceberg**: ClickHouse's Iceberg support is immature and read-only. Keep native MergeTree tables. Revisit at 1TB+ scale.
  - **No AWS S3 / BigQuery for L2s**: These don't exist for Base/Arbitrum/Optimism. Only Ethereum is in BigQuery public datasets.
  - **Bridge linking scope**: Base-only bulletproofing in Week 4-5. Arbitrum/Optimism as preview. Stargate/OFT proven on Base only.
  - **Weekly architectural guardrails**: Three tests every Friday — /core standalone test, fake-integrator test, schema extensibility test.

Files touched:
  - Created: `PRODUCT_PLAN.md` (18-month arc, 3 product surfaces, 5 Web2 validation interviews)
  - Created: `ENGINEERING_PLAN.md` (monorepo structure, canonical schema DDL, 6-week sprint)
  - Created: `README.md`, `docker-compose.yml`, `.gitignore`, `Makefile`
  - Created: `core/schemas/canonical_events.sql`, `entity_relationships.sql`, `bridge_links.sql`, `registry.yaml`, `validator.py`
  - Created: `core/adapters/base.py`, `evm/__init__.py`, `ga4/__init__.py`, `posthog/__init__.py`, `dummy/__init__.py`
  - Created: `core/identity/graph.py`, `core/trajectory/engine.py`
  - Created: `core/tests/standalone/test_core_standalone.py`, `fake_integrator/test_web2_trajectory.py`, `extensibility/test_schema_extensibility.py`
  - Created: `avail/nexus_cs/heuristics/engine.py`, `avail/fastbridge/attribution/engine.py`, `avail/gtm/scoring/engine.py`
  - Created: `avail/nexus_cs/README.md`, `avail/fastbridge/README.md`, `avail/gtm/README.md`
  - Created: `core/pyproject.toml`, `avail/pyproject.toml`
  - Created: `ops/ci/weekly_arch_tests.sh`
  - Created: `.claude/facts.md`, `.claude/journal.md`
  - Archived: `conv.md`, `conv2.md`, `plan.md`, `reconciled_plan.md` → `/archive/`
  - Updated: `ENGINEERING_PLAN.md` (corrected data sources: removed AWS S3/BigQuery for L2s, added HyperSync, Pocket Network)

Open threads:
  - EVM ingestion adapter not yet implemented (`core/adapters/evm/__init__.py` is a stub)
  - HyperSync client integration not yet tested
  - Week 1 EVM ingestion not started
  - Need to test `docker-compose up` with ClickHouse + Postgres + Dagster + Observable
  - 5 Web2 validation interviews not scheduled
  - Need to close remaining tk preconditions and start P1 tickets

Tickets created (14 total):
  - Closed: na-x72o (IP conversation), na-9zb7 (monorepo scaffold), na-12wg (canonical schema)
  - Ready: na-pza3 (EVM adapter), na-t68g (identity graph), na-fnuh (Web2 adapters), na-kzzw (arch tests), na-6co7 (Web2 interviews)
