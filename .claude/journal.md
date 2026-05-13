# Journal

## 2026-05-13 — ReplacingMergeTree migration + Phase B decoder grind

Worked on: Closed the sink-dedup open thread by migrating
canonical_events, canonical_logs, bridge_links to ReplacingMergeTree
(engine handles dedup at merge time). Generalized the
re-decode-from-raw workflow into ops/redecode.py and shipped 6 new
decoders end-to-end on prod (aave_v3, lido, spark, compound_v3,
morpho_blue, cctp).

Decisions:
  - **3 event tables → ReplacingMergeTree.** ORDER BY tuples chosen
    for content-identical dedup; canonical_logs PARTITION BY chain
    (was toYYYYMM(inserted_at)) so re-ingest hits the same partition
    and merges actually collapse dupes.
  - **FINAL on every canonical_events / bridge_links read in
    api/queries.py.** Free in steady state (~120ms identical on dev).
    Necessary because ReplacingMergeTree is eventually consistent.
  - **Re-decode-from-raw is the right Phase B unblock.**
    canonical_logs is the unfiltered HyperSync mirror; new decoder
    = YAML + seed_<protocol>_contracts.py + `python ops/redecode.py
    --protocol <slug>`. 6 decoders shipped today; prod dashboard
    non_swap_defi went 0 → 10,100 on 2d window.
  - **`.tickets/` is gitignored** — local-only convention. Durable
    cross-machine artifacts are commits + facts.md + journal.md.
  - **tk epic na-2haa** tracks BD requirements finish-out + remaining
    LRT/perp/bridge leaves.

Files touched: core/schemas/clickhouse/*, core/sink/clickhouse.py,
api/queries.py, core/adapters/evm/decoders/mappings/{aave_v3, lido,
spark, compound_v3, morpho_blue, cctp}.yaml, ops/{redecode.py,
migrations/2026-05-13_replacing_mergetree.sql,
run_replacing_mergetree.sh, seed_aave_v3_contracts.py,
seed_lido_contracts.py, seed_spark_contracts.py,
seed_compound_v3_contracts.py, seed_morpho_blue_contracts.py,
seed_cctp_contracts.py}, core/tests/standalone/test_generic_decoder.py.

Open threads:
  - CCTP bridge_in (MessageReceived + MintAndWithdraw multi-log
    handler — needs hand-written class, not YAML).
  - LRT (Renzo / Kelp / EtherFi) — at least one needed for na-2haa.
  - Perps — GMX V2 needs hand-written EventEmitter class (topic1
    dispatch pattern).
  - 3 more bridges to satisfy "at least 4" criterion (Wormhole,
    Mayan, Hyperlane, Synapse, Hop, deBridge, Polygon PoS).
  - 30-day backfill still paused; cron still disabled.
  - Frontend hasn't been rebuilt since today's API changes (still
    works — FINAL changes are server-side only).
  - SSH-via-bash run_in_background batches its output until session
    close, so deploys look slow even though work is fast. Consider
    streaming via `ssh -t` or a Monitor-style poller next time.

## 2026-05-13 — Stress-tested perf stack; sink dedup is the next bottleneck

Worked on: Second-pass perf wins on the cron pipeline (threading-safe API
client, nginx 5-min cache, CH ingest throttle, parallel chain ingest,
second_hop CTE filter, aggregator dedup count fix, token_metadata seeder).
Resumed 30d backfill, observed cron+backfill contention (4h 42m ticks
under load), paused backfill, disabled cron mid-investigation.

Decisions:
  - **Cron disabled** in crontab (timestamped comment) until backfill
    clears. Cron+backfill concurrent → both ~3-4× slower.
  - **Sink dedup pattern is the next perf lever**: per-1000-event
    SELECT-then-filter does ~3000 round-trips per polygon chunk × ~2s
    under load = 1.5h+ wall just for polygon writes. For backfill into
    mostly-empty days, every SELECT returns 0 — pure waste. Need
    `--no-dedup` flag for backfill OR migrate canonical_events to
    ReplacingMergeTree.
  - **API responsiveness solved**: per-thread CH client (commit 89e5b4a)
    + nginx 5-min cache (9652f8d) + ingest throttle (e02985f). First
    hit pays CH cost, refreshes sub-200ms even mid-tick.

Files touched: core/sink/clickhouse.py, api/{ch,queries,main}.py,
ops/dagster/nexus_pipeline/assets.py, ops/{nginx/nexus.conf,
run_ingestion.py, seed_token_metadata.py, import_dune_contracts.py},
frontend/src/{lib/api.ts, pages/BridgeFlow.tsx}.

Open threads:
  - Re-enable cron after backfill done (commented in crontab).
  - Sink-dedup structural fix: --no-dedup flag for backfill OR
    ReplacingMergeTree migration.
  - Dynamic token_metadata discovery for long-tail tokens.
  - Backfill currently paused mid-window (2026-04-12 → 2026-05-12).

## 2026-05-11 — Phase D: VPS deploy live at analytics.themuse.one

Worked on: na-7pax (Phase D / na-7715 epic). Shipped the BD MVP to
shieldtx-vps. Site is live at https://analytics.themuse.one behind
Cloudflare Access (email allowlist).

Decisions:
  - **Cloudflare Tunnel + Access, not nginx + LE + basic-auth**: VPS
    has no free public 80/443 (devpush Traefik occupies 8090/8443) and
    cloudflared is the path of least resistance for auth-gated public
    HTTPS. Ditched the original facts.md plan same-day.
  - **Self-contained `docker-compose.prod.yml`**: doesn't extend the
    dev compose. Skips dagster/observable, runs nexus-{ch,pg,api,nginx}
    on shifted host-loopback ports (18080/18123/19000/15434) so it
    coexists with shieldtx-{ch,pg} without collision.
  - **API in Docker (slim Python 3.12 image), frontend built on the
    VPS via pnpm + corepack**: nginx container reverse-proxies /api/*
    to the api container and serves the static SPA from `frontend/dist`.
  - **Schemas split into clickhouse/ + postgres/ subdirs** with all CH
    tables qualified `nexus.X` and a leading `00_database.sql`. Was
    a pre-existing latent bug — old layout would crash CH init on a
    fresh volume because 6/13 .sql files were Postgres-flavored.
  - **Cloudflare Access via One-Time PIN (email magic link)** —
    initial choice. Gate intercepted requests cleanly per curl tests,
    but the OTP email never delivered (Gmail + a fresh CF One account
    on team `rakesh145`; even after adding the documented
    `Login Method = One-time PIN` Require rule). Likely a CF
    suppression list issue.
  - **Reverted to nginx HTTP basic-auth same day**: `auth_basic` block
    in `ops/nginx/nexus.conf`, apr1-hashed htpasswd at
    `ops/nginx/htpasswd` (gitignored, user `nexus`). CF Access app
    deleted; Cloudflare Tunnel still terminates TLS.

Bugs found + fixed mid-deploy:
  - core/pyproject.toml not pip-installable → installed deps directly,
    use PYTHONPATH=. (matches dev convention).
  - run_ingestion.py + run_backfill.py hardcoded dev port 8124 / 5434
    even with .env present → patched to honor CLICKHOUSE_*  +
    PROTOCOL_CONTRACTS_DSN env vars. Commit `921a54e`.
  - import_dune_contracts.py loads dotenv AFTER argparse defaults →
    workaround was passing --postgres explicitly. Same pattern in
    run_ingestion/run_backfill (still latent for cron). Recorded as a
    Gotcha in facts.md.
  - eth-hash needs pycryptodome backend on a fresh venv — not in
    core/pyproject.toml's deps. Added at install time.

Files touched:
  - api/Dockerfile, docker-compose.prod.yml, ops/nginx/nexus.conf,
    docs/DEPLOY.md, .env.example (commit 639a9e6).
  - core/schemas/{clickhouse,postgres}/* split + nexus.* qualifier +
    00_database.sql (commits f233c89, c3fab87).
  - ops/run_ingestion.py + ops/run_backfill.py env-aware (921a54e).
  - On VPS only: /etc/cloudflared/{config.yml,4639ebdb-….json},
    crontab entry, tmux session `backfill`.

State at end of session:
  - 5-min smoke ingestion landed 256k events + 17 bridge_links.
  - Dune bootstrap landed 23k contracts + 23k labels into
    protocol_contracts/contract_labels.
  - 30-day backfill running in tmux session `backfill` on the VPS
    (~30h ETA). Logs at `~/nexus-analytics/logs/backfill.log`.
  - Hourly cron installed (next tick top-of-hour). Will overlap with
    the backfill — both are idempotent so harmless.

Open threads:
  - **Backfill paused** mid-run. min(canonical_events.timestamp) is
    2026-04-11 13:05 (got most of the 30d window), max is
    2026-05-11 15:53 (cron-pulled recent data). Resume with
    `tmux new -d -s backfill ...` once cron stabilises (idempotent,
    safe to re-run any window).
  - **Bridge_links not running in cron anymore** (`--skip-bridge-links`
    in the tuned cron). Needs separate cadence — either a 4-hourly
    cron that runs `--force` over a wider lookback, or a Dagster
    schedule. The slow per-pending-bridge CH lookup makes this
    expensive enough that we don't want it inline with hourly.
  - **Profile bridge_links** — ~5 min for 17 matches is ~17s per
    pending bridge, suggesting a non-batched query. Worth a
    `tldr cfg` + a CH `system.query_log` look.
  - Move load_dotenv() before argparse defaults in run_ingestion,
    run_backfill, import_dune_contracts so prod doesn't depend on
    `set -a; source .env; set +a` shell hygiene.
  - Phase B work (15 single-protocol decoders + Curve + Balancer + 8
    bridges) still parallelisable post-launch. Ticket epic na-09db.

Late-session tuning (post-launch):
  - 4 cron ticks observed alive concurrently after Phase D landed —
    hourly @ --lookback 60 takes ~2h wall (raw_logs phase alone =
    36m for ~6M raw logs across 5 chains).
  - Killed all stale ingestion procs + paused backfill (was
    competing with cron for HyperSync — connection-reset retry
    storms in backfill log).
  - New crontab line:
    `0 * * * * flock -n /tmp/nexus-ingest.lock bash -c 'cd ~/nexus-analytics && PYTHONPATH=. .venv/bin/python ops/run_ingestion.py --lookback 90 --skip-bridge-links' >> ~/nexus-analytics/logs/ingest.log 2>&1`
  - Next tick: 18:00 UTC. Observe 2-3 ticks before deciding whether
    to widen cadence to every 2h.

## 2026-05-12 — Perf audit: decoded_events 3h→1h35, bridge_links 5m→10s

Worked on: Investigated why hourly cron at --lookback 60 was taking
3-4h. Found two architectural under-utilisations of CH:

  1. **decoded_events re-fetched HyperSync** — the asset called
     `chain_adapter.ingest_raw(start, end)` (full unfiltered window
     just pulled by raw_logs), then decoded in Python and dropped
     ~70% of logs. The codebase already had
     `chain_adapter.ingest(start, end)` which pushes the topic0
     filter to HyperSync's edge. Asset wasn't using it.

  2. **bridge_links matching was a Python loop with per-bridge_in
     Postgres roundtrips** (4-5 min for ~17 matches). Both bridge_out
     and bridge_in were already in canonical_events — the link is a
     CH JOIN on link_key.

Decisions:
  - **Filtered ingest via `chain_adapter.ingest()`**: single fetch
    per chain, only topic0-known logs, decoded inline. Commit
    `055dfb8`.
  - **`bridge_links` via CH JOIN**: single `INSERT INTO bridge_links
    SELECT … FROM canonical_events bo INNER JOIN canonical_events bi
    ON bo.link_key = bi.link_key` with anti-join for idempotency.
    Commit `af39f7c`. First run materialised 970 links in 21s (was
    17 after many days of Python-loop matching).
  - **Aggregator dedup wired** — `sink.dedup_aggregators()` existed
    but was never called. Now invoked at end of decoded_events.
    2,448 swap_internal rows now in canonical_events. **Cosmetic
    bug**: returns 0 count because CH ALTER UPDATE is async; the
    actual mutation works. Commit `eb68071`.
  - **Drop `--skip-bridge-links` from cron, add `--skip-raw-logs`
    and `--skip-prices`**. bridge_links via JOIN is so cheap (~10s)
    it goes back in; raw_logs is redundant now that decoded_events
    fetches its own filtered logs; token_metadata is empty so
    token_prices is 47s of no-op overhead.
  - **Cross-chain funnel API + UI**: two new endpoints
    (cross-chain-matrix, completion) + a section on the /bridge
    page surfacing the materialised links. Real link rate: 44.7%.
    Commits `4d56bec`, `cc44b1c`.
  - **Fix load_dotenv ordering** in `import_dune_contracts.py`
    (others were already correct from commit 921a54e). Commit
    `a7e0d45`.

First measurement (08:00 UTC tick): **1h35m total** (was 3-4h).
decoded_events alone 95 min (was 3h18m on 04:00 UTC tick).
bridge_links 9.6s (was 4-5 min). Polygon dominates (~57 min of
decoded_events) — chain-parallel ingest is the next perf lever.

Open threads:
  - Polygon dominates decoded_events. Run chains in parallel
    (asyncio or threads) for ~30 min savings.
  - `token_metadata` is empty → USD enrichment never runs → all
    `amount_usd` are 0. Wire `TokenMetadataLoader` into a cron job.
  - Resume the 30-day backfill (paused yesterday). Now safe to
    run alongside cron since cron leaves more headroom.
  - Dedup_aggregators() returns 0 even when work happens.
    Cosmetic fix: poll system.mutations or do a count-before /
    count-after.

## 2026-05-11 — BD MVP: pipeline, API, frontend, parity dig

Worked on: Closed na-rk8b (daily spikes), na-hmeu (parity validator +
dex-priority resolver + Solidly fix), na-s7d8 (template audit), na-5run +
na-lh5q (UniV4 multi-chain). Shipped Phase A (cron ingestion), Phase C.1
(FastAPI 13 endpoints), Phase C.2 (React+Vite frontend), MVP roadmap doc.

Decisions:
  - **Cron over Dagster**: dagster dev never scheduled reliably. ops/run_ingestion.py
    runs identically on laptop + VPS.
  - **Resolver priority (is_manual, contract_type, source)**: was source-only.
    Dex beats aggregator — fixes Aerodrome misrouting to bitget_dex_aggregator.
  - **Solidly_v1 parent**: Aerodrome v1 + Velodrome v2 use
    Swap(address,address,uint256,…), not UniV2. Audit confirmed no other forks
    mis-templated.
  - **MVP-first**: ship live URL with current decoder coverage, fill Phase B
    post-launch. React+Vite, no shadcn dep.
  - **VPS deploy spec**: analytics.themuse.one on shieldtx-vps, nginx+LE,
    basic-auth, CH+PG bind 127.0.0.1.

Files touched: core/registry/{protocol_contracts,dune_bootstrap,uniswap_v4_pools}.py,
decoders/mappings/solidly_v1.yaml + 2 retemplates, ops/{run_ingestion,
run_backfill,audit_dex_mapping_templates,validation/dune_parity,
backfill_uniswap_v4_pools}.py, ops/CRON.md, observable/src/data/spikes.json.py,
api/, frontend/, docs/MVP_ROADMAP.md.

Open threads:
  - Phase D VPS deploy (.tickets/na-7pax.md).
  - na-lh5q Arb+Op UniV4 pool backfill (~90 min unattended).
  - Phase B: 15 single-protocol decoders + Curve + Balancer + 8 bridges + 30d backfill.

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
