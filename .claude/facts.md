# Nexus Analytics — Facts

**Last reconciled:** 2026-05-13 (ReplacingMergeTree migration + Phase B decoder grind)

## Architecture

[2026-04-23] Monorepo split: `/core` (MIT-destined), `/avail` (proprietary), `/commercial` (future enterprise tier).
[2026-04-23] Canonical event schema uses `entity_id` + `entity_type` (not wallet-specific) for horizontal compatibility.
[2026-05-13] canonical_events ORDER BY `(entity_id, timestamp, event_id)` — ReplacingMergeTree dedup key. The `(entity_id, timestamp)` prefix preserves trajectory query plans via CH's prefix-matching.
[2026-04-23] Schema registry in `/core/schemas/registry.yaml` — adding event types requires registry entry + adapter + test.
[2026-04-23] Trajectory primitive `trajectory(entity_id, anchor, window_before, window_after)` is the core query. Target: <2s for 30-day window.
[2026-04-23] Identity graph uses `entity_relationships` table (not flat `wallet_identity`). Supports multi-hop resolution with confidence scores.
[2026-05-13] All 3 event tables are ReplacingMergeTree. canonical_events ORDER BY (entity_id, timestamp, event_id); canonical_logs ORDER BY (chain, block_number, tx_hash, log_index) PARTITION BY chain (re-ingest hits the same partition so merges collapse dupes); bridge_links ORDER BY (link_key_type, link_key, src_chain, dst_chain, src_event_id, dst_event_id). Engine handles dedup at merge time — no more pre-insert SELECTs. No Iceberg in v1; revisit when data exceeds 1TB.
[2026-05-13] Two ingest paths in EVMAdapter: `ingest_raw` uses HyperSync `LogSelection()` (no topic filter) → canonical_logs (raw mirror, opt-in via cron --skip-raw-logs); `ingest` uses `LogSelection(topics=[registry.all_topic0s()])` → canonical_events (decoded, dashboard reads this). canonical_logs is the unfiltered mirror used for "add a new decoder, re-decode history" workflows.
[2026-05-13] api/queries.py uses `FROM canonical_events FINAL` (and `FROM bridge_links FINAL`) on every read. Required for clean aggregates over ReplacingMergeTree. Steady-state cost ~120ms (identical to non-FINAL post-merge). FINAL on plain MergeTree errors with Code 181 — engine migration must precede deploying FINAL-using code.
[2026-05-13] Re-decode-from-raw workflow: new decoder = YAML mapping under `core/adapters/evm/decoders/mappings/<slug>.yaml` + `ops/seed_<protocol>_contracts.py` + `python ops/redecode.py --protocol <slug>`. Reads canonical_logs, decodes through new YAML, writes canonical_events. Idempotent via engine-level dedup. 5-15 min per protocol.
[2026-05-13] Non-swap decoders shipped via re-decode: aave_v3, lido, spark, compound_v3, morpho_blue. CCTP V1 ships bridge_out (DepositForBurn) only; bridge_in needs multi-log handler. Tracked under tk epic na-2haa.
[2026-05-06] ClickHouse database is `nexus` (not `default`). All SinkConfig, Dagster resources, and enrichment configs use `nexus`.
[2026-05-06] Bridge matching uses Postgres `pending_bridge_outs` table with retry scheduling (ADR-001 implemented).
[2026-05-06] OP Stack L2→L1 withdrawals use withdrawalHash for precise matching (ADR-002 implemented).
[2026-05-06] Aggregator dedup: swap → swap_internal when aggregator event exists in same tx.
[2026-05-06] Stablecoin override at ingestion: 22 tokens across all 4 chains always resolve to $1.00.
[2026-05-06] USD at ingestion via PriceResolver: CoinGecko primary, DexScreener zero-auth fallback.
[2026-05-08] Generic ABI decoder framework: per-protocol YAML mapping under `core/adapters/evm/decoders/mappings/{protocol}.yaml` + optional plugin in `plugins.py` replaces hand-written `LogDecoder` subclasses for one-log-to-one-event protocols. Bespoke classes reserved for bridges/aggregators with stateful or multi-log logic.
[2026-05-08] Address-first decoder lookup: `DecoderRegistry.lookup(topic0, address, chain)` tries (chain, address) → protocol → YAML mapping first, then falls back to topic0. Required for shared-topic0 disambiguation (UniV2 vs Sushi vs Pancake).
[2026-05-08] Address registry: Postgres `protocol_contracts` (chain, address, protocol, version, contract_type, source) with composite PK on (chain, address, source). Sources coexist; lookup priority manual > dune > spellbook.
[2026-05-08] Label registry: Postgres `contract_labels` (chain, address, label, category, source). Populated by JOIN of Dune `labels.addresses` against known address universe — never unfiltered.
[2026-05-08] ABI cache: Postgres `protocol_abis` (code_hash PK, abi_json, source). Bytecode-hash dedup means factory-deployed pools share one ABI entry.
[2026-05-08] Bytecode index: Postgres `contract_bytecodes` (chain, address PK, code_hash, is_proxy, implementation_address). EIP-1967 proxies resolved at fetch time so the ABI cache key is the impl's hash, not the proxy's.
[2026-05-11] BD MVP frontend at `frontend/` (React 19 + Vite 8 + Tailwind 4 `@theme` + recharts + tanstack-query). FastAPI backend at `api/`. Dev: Vite proxies `/api/*` to FastAPI on 8000. Prod: same-origin behind nginx.
[2026-05-11] `make_cached_resolver` priority is `(is_manual, contract_type, source)` — `dex` beats `aggregator` for the same address regardless of source. Fixes Dune dual-labelling (same pool tagged in both `dex.trades` and `dex_aggregator.trades`).
[2026-05-11] Dune bootstrap tags `protocol_contracts.contract_type` ∈ {dex, aggregator, bridge, pool_manager} based on source table.
[2026-05-11] Solidly forks emit `Swap(address indexed sender, address indexed to, uint256[4])` — topic0 `0xb3e2773606...`, NOT UniV2's `0xd78ad95f...`. `mappings/solidly_v1.yaml` parent + 2 retemplated forks (aerodrome_v1, velodrome_v2). Audit (ops/audit_dex_mapping_templates.py) confirmed no other bulk-port mis-templates.
[2026-05-11] UniV4 PoolManager `Swap` carries PoolId only (no token addresses). `uniswap_v4_pools` Postgres registry populated from `Initialize` events by `ops/backfill_uniswap_v4_pools.py`. Lazy resolver in `core/adapters/evm/decoders/plugins.py:_univ4_pool_resolver` (preloaded dict).
[2026-05-11] YAML `template: <parent>` field — child mappings inherit `events` + plugin from a parent (uniswap_v2 / uniswap_v3 / solidly_v1). 84 fork mappings generated by `ops/bulk_port_dex_decoders.py`.
[2026-05-11] Prod stack on shieldtx-vps lives at `/home/apnetv/nexus-analytics`. `docker-compose.prod.yml` runs nexus-{ch,pg,api,nginx} bound to 127.0.0.1 on shifted ports (18080 nginx · 18123/19000 ClickHouse · 15434 Postgres) so it doesn't collide with the host's existing shieldtx-{ch,pg} containers.
[2026-05-11] Schemas split into `core/schemas/clickhouse/` (7 files, MergeTree) and `core/schemas/postgres/` (6 files, JSONB/SERIAL). `00_database.sql` runs first alphabetically and `CREATE DATABASE IF NOT EXISTS nexus`. All ClickHouse `CREATE TABLE` statements are qualified with `nexus.` so docker-entrypoint init lands tables in the right DB regardless of session-default.
[2026-05-11] Edge ingress on shieldtx-vps is **Cloudflare Tunnel**, not nginx + LE (no public 80/443 free). Tunnel `nexus-analytics` (id 4639ebdb-10ae-4a14-814d-6525d6a6254c), config at `/etc/cloudflared/config.yml`, systemd unit `cloudflared.service`. Routes `analytics.themuse.one` → `http://127.0.0.1:18080`.
[2026-05-11] Access gate is **nginx HTTP basic-auth** (`auth_basic` block in `ops/nginx/nexus.conf`, htpasswd at `ops/nginx/htpasswd` on the VPS only — gitignored). Reverted same-day from Cloudflare Access OTP because the OTP email never delivered to a fresh CF One account on team `rakesh145` (likely Gmail/CF suppression interaction). User: `nexus`. Cloudflare Tunnel still terminates TLS; CF Access app deleted.
[2026-05-11] Frontend uses **pnpm** (not npm). VPS deploys: `corepack enable && pnpm install --frozen-lockfile && pnpm run build`. `npm ci` fails (no `package-lock.json`).

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
[2026-05-11] BD MVP is a phased path — see `docs/MVP_ROADMAP.md`. Phase A (pipeline) + C.1/C.2 (API + UI) done; Phase D (VPS deploy) next; Phase B (decoder coverage) is parallelisable post-launch.
[2026-05-11] Ingestion runs via cron (`ops/run_ingestion.py`), NOT a Dagster daemon. Dagster `@asset` functions remain in `ops/dagster/nexus_pipeline/` as DAG documentation; `dagster.materialize()` is invoked once per cron tick.
[2026-05-11] "Meaningful DeFi action" filter in `api/queries.py` excludes ERC20 transfer/approval setup noise. Used for `first-action` / `swap-vs-non-swap` / `activity-24h` queries so BD output is about apps, not token plumbing.
[2026-05-13] Decoder seed convention: `source="manual"` + `version=None` → slug = protocol = YAML's `protocol:` field. Setting `version="N"` produces slug `"{protocol}_vN"` which requires a matching YAML. Manual beats Dune/Spellbook in resolver priority.
[2026-05-13] `token_in` / `token_out` can be omitted in YAML canonical mappings when the asset is implicit in the venue (Compound V3 Comet, Morpho Blue market id). registry.yaml's "required_properties" is documentation; schema columns are Nullable.

## Gotchas

[2026-04-23] HyperSync free tier is primary data source. Pocket Network public RPC is free fallback. cryo is tertiary.
[2026-04-23] No AWS S3 public datasets for Base/Arbitrum/Optimism. BigQuery only has Ethereum.
[2026-05-06] HyperSync URL: `eth.hypersync.xyz` for Ethereum (NOT `ethereum.hypersync.xyz`).
[2026-05-06] Across V3FundsDeposited/FilledV3Relay are LEGACY unused events. Active events: FundsDeposited/FilledRelay with bytes32 address types.
[2026-05-06] Across SpokePool address on Base: `0x09aea4...bec64` (NOT `0x09aea4...8B8EF6`).
[2026-05-06] Stargate ReceiveFromChain topic0 is WRONG — 0 events on any chain. Bridge_in is LayerZero PacketDelivered.
[2026-05-06] LayerZero V1 IDs (Spellbook: 101=eth, 184=base) differ from V2 EIDs (30101=eth, 30184=base). Stargate SendToChain uses EVM chain IDs, ReceiveFromChain uses V2 EIDs.
[2026-05-13] clickhouse-connect returns FixedString columns as `bytes`, not str. Decode with `.decode('ascii')` at the row boundary before passing to anything expecting hex strings. Pattern: `_s(v)` helper in ops/redecode.py.
[2026-05-13] FINAL on plain MergeTree → `Code: 181 — Storage MergeTree doesn't support FINAL`. Engine migration must precede deploying queries that use FINAL.
[2026-05-13] `EXCHANGE TABLES` on CH 24+ Atomic DB is atomic and safe under live read traffic. The 5-min nginx cache hides cold-query latency during the rename.
[2026-05-13] Bridges and aggregators need hand-written decoder classes when matching requires multi-log or stateful logic. CCTP bridge_in (MessageReceived + MintAndWithdraw in one tx), GMX V2 (EventEmitter with topic1 dispatch) are open under na-k7h7 / na-qx89.
[2026-05-13] Morpho Blue's Supply event has 3 indexed args (id, caller, onBehalf), not 2. Verified from on-chain log shape (3 topics + 2 uint256 in data). Some docs assume 2.
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
[2026-05-11] UniV4 PoolManager addresses (verified): ethereum 0x000000000004444c5dc75cb358380d2e3de08a90, base 0x498581ff718922c3f8e6a244956af099b2652b2b, arbitrum 0x360e68faccca8ca495c1b759fd9eee466db9fb32, optimism 0x9a13f98cb987694c9f086b1f5eb990eea8264ec3, polygon 0x67366782805870060151383f4bbff9dab53e5cd6.
[2026-05-11] Etherscan V2 free tier supports Ethereum/Arbitrum/Polygon but NOT Base/Optimism. For backfills on those, use `from_block=0` — HyperSync's (address, topic0) filter keeps wide scans cheap.
[2026-05-11] UniV4 on Base has ~3.27M `Initialize` events (cheap hook-spam pools). Use `psycopg2.extras.execute_values` for the upsert; naive `executemany` is ~10× slower.
[2026-05-11] `load_dotenv()` MUST run before `argparse.add_argument(default=os.getenv(...))` or argparse captures the unloaded value. Existing scripts (run_ingestion, run_backfill, import_dune_contracts) violate this — workaround for now is `set -a; source .env; set +a` before invocation OR pass the var explicitly with `--postgres "$PROTOCOL_CONTRACTS_DSN"`.
[2026-05-11] `core/pyproject.toml` is NOT pip-installable (hatchling can't auto-discover packages — no `nexus_core/` dir matching the project name). Repo runs with `PYTHONPATH=.` instead. On the VPS we install only the deps list manually (`pip install dagster hypersync clickhouse-connect ...`) plus `eth-hash[pycryptodome]`.
[2026-05-11] 5-min lookback ingestion takes ~11 min wall on shieldtx-vps (53k base + 13k eth + 0.8k arb + 18k op + 171k polygon ≈ 256k events; bridge_links step alone is 4-5 min for ~17 matches). Hourly cron at `--lookback 60` will self-overlap; tune to shorter lookback or `--skip-bridge-links` once cron behavior is observed in prod.
[2026-05-11] Confirmed in prod: hourly cron at `--lookback 60` permanently overlaps itself (4 ticks alive concurrently, raw_logs alone took 36m for one tick fetching ~6M raw logs across 5 chains). Tuned to `flock -n /tmp/nexus-ingest.lock` + `--lookback 90 --skip-bridge-links`. flock guarantees no overlap (subsequent ticks no-op silently if lock held); 90-min lookback covers the slack. Bridge_links is now an open thread — needs separate cadence (see journal). Backfill must NOT run concurrently with cron — both compete for HyperSync.
[2026-05-12] Architectural perf fix: `decoded_events` asset was calling `chain_adapter.ingest_raw(start, end)` (full unfiltered HyperSync fetch — same window already pulled by `raw_logs`), then decoding in Python and dropping ~70% of logs. Switched to `chain_adapter.ingest(start, end)` which sends `LogSelection(topics=[known_topic0s])` to HyperSync — single fetch, only relevant logs, decoded inline. Expected wall-time: 3-4h → ~30 min per cron tick. Cron flag `--skip-raw-logs` skips the (now redundant) raw_logs phase entirely; raw_logs stays available for the rare "add a new decoder, re-decode history" workflow. Commit `055dfb8`.
[2026-05-12] `bridge_links` asset now materialised via a single CH JOIN (`canonical_events bo INNER JOIN canonical_events bi ON bo.link_key = bi.link_key` with anti-join against `bridge_links` for idempotency). Replaced the prior Python loop + per-bridge_in PG roundtrip path. First run on existing data: 17 → 970 links in 21s. The PG `pending_bridge_outs` writes from `decoded_events` are now dead state — to be cleaned up. Commit `af39f7c`.
[2026-05-12] API queries (`api/queries.py`) hit `canonical_events` with `event_type='bridge_out'/'bridge_in'` directly — they do NOT use the `bridge_links` table. So the /bridge page was never blocked on bridge_links. The materialised `bridge_links` rows unlock NEW analyses (cross-chain funnel, bridge completion rate, FastBridge GA4→bridge→swap funnel).
[2026-05-12] Cross-chain funnel shipped: `GET /api/bridge-flow/cross-chain-matrix` (src→dst aggregates with bridges/wallets/avg+p50 latency) and `GET /api/bridge-flow/completion` (matched% over bridge_outs). UI section on /bridge page powered by `bridge_links`. Real numbers from first 30d window: 44.7% link rate (970 of 2170 bridge_outs matched), top routes eth↔base + base→ethereum, latencies 2-200s depending on bridge family.
[2026-05-12] First definitive measurement of the new architecture on cron tick 08:00 UTC: 1h35m total (was 3-4h). decoded_events alone was 95 min (~2.1× faster than old 3h18m). bridge_links via CH JOIN: 9.6s (was 4-5 min). raw_logs + token_prices skipped entirely (saved 36m + 47s). Polygon dominates decoded_events at ~57 min — chain-parallel ingest is the next perf lever.
[2026-05-13] Aggregator dedup wired (commit eb68071) and verified correct (saw "reclassified 781 swap → swap_internal" in the 14:00 UTC May 12 tick log; ~3.2k swap_internal rows total). Count fix (commit 5c455c3) does a pre-mutation SELECT before the async `ALTER UPDATE` so the log line shows the real number.
[2026-05-13] `token_metadata` seeded with 13 hardcoded major tokens (WETH/USDC/USDT/DAI/WBTC across eth/base/arb/op + a few extras) via `ops/seed_token_metadata.py` (commit 2ec8a16). Re-runnable safely (table is ReplacingMergeTree on (chain, address)). Polygon's `load_hardcoded` list is empty. **Open thread**: dynamic discovery for long-tail tokens — current state is amount_usd = 0 for unknown tokens.
[2026-05-12] **`api/ch.py` thread-safety bug fixed** (commit `89e5b4a`): was `@lru_cache(maxsize=1)` on the clickhouse-connect client. FastAPI threadpool workers shared one client → concurrent dashboard requests tripped "Attempt to execute concurrent queries within the same session". Switched to `threading.local` so each worker thread gets its own client.
[2026-05-12] **CH ingest throttle** (commit `e02985f`): every sink connection (ClickHouseSink, RawLogSink, BridgeLinkSink) now uses `settings={'max_threads': 4, 'max_insert_threads': 2, 'priority': 10}`. Caps ingest's CH CPU footprint and lets API queries (default priority=0) jump the queue. Trade-off: ingest somewhat slower per tick but dashboard stays responsive.
[2026-05-12] **nginx 5-min response cache** for `/api/*` (commit `9652f8d`): proxy_cache + serve-stale-on-error. First panel load is the CH cost; everything after is sub-200ms from disk. `proxy_cache_use_stale` keeps the dashboard populated even mid-cron-tick. Cache is at `/var/cache/nginx/api`, 200MB cap, 30min inactive eviction. `X-Cache-Status` header exposed.
[2026-05-12] **Parallel chain ingest** (commit `b04d30b`): decoded_events asset now runs all 5 chains via `asyncio.gather + asyncio.to_thread`. Without throttle, this maxes CH CPU for the duration. With throttle (above) + cache, the trade-off is balanced. Earlier sequential pattern: 95min wall (polygon dominates 57min). Parallel: ~57min wall.
[2026-05-12] **second_hop_after_swap query** (commit `0cad49a`): added `event_type IN (_MEANINGFUL_LIST)` filter on the `next_events` CTE — previously scanned essentially the whole canonical_events table (~22M rows in 9d). Helped (49s → 35s) but cache made the rest of the optimisation moot.
[2026-05-12] Daily event distribution at backfill resume time: 4 days had ANY data (2026-04-11 1.4M, 2026-05-07 9k, 2026-05-11 8.7M, 2026-05-12 8.3M). 24+3 days with zero. Backfill window 2026-04-12 → 2026-05-12 in flight (tmux session `backfill`).
[2026-05-13] **Sink dedup pattern is the next perf bottleneck.** ClickHouseSink/RawLogSink/BridgeLinkSink call `_deduplicate(events)` on every flush — chunked SELECT against existing event_ids before INSERT. ~3000 round-trips per polygon chunk × ~2s under contention = ~1.5h wall just for polygon writes. **For backfill into mostly-empty days, 100% of dedup SELECTs return 0 — pure waste.** Either: `--no-dedup` flag for backfill mode, OR migrate `canonical_events` to ReplacingMergeTree (event_id-keyed) so dedup happens at merge time / via FINAL on read. Until fixed, backfill takes ~3-4h per cron-equivalent chunk and is infeasible at 30d × 5 chains × hourly granularity.
[2026-05-13] Cron+backfill must be mutually exclusive in prod. Concurrent runs make both ~3-4× slower (HyperSync free-tier rate limits + CH write contention; observed 4h 42m for one cron tick that should have been 60-90 min). Either run via shared flock OR alternate via crontab schedule. Cron currently disabled in crontab (timestamped comment) while backfill catches up.

## Dependencies & Tooling

[2026-05-08] HyperSync URLs (verified live with token): eth.hypersync.xyz, base.hypersync.xyz, arbitrum.hypersync.xyz, optimism.hypersync.xyz, **polygon.hypersync.xyz**.
[2026-04-23] ClickHouse + Postgres via Docker Compose. Dagster for orchestration. Observable Framework for developer-facing analytics dashboards.
[2026-04-23] Docker Compose stack includes: ClickHouse, Postgres, Dagster, Observable Framework.
[2026-05-13] `tk` CLI for ticket tracking — tickets at `.tickets/<id>.md`, **gitignored locally**. Tickets survive across sessions on the same machine but NOT across clones. Cross-machine durability for plans/decisions lives in `.claude/facts.md` + `.claude/journal.md`.
[2026-05-13] BD finish-out tracking epic: tk `na-2haa` documents the 7-step decoder workflow + remaining LRT/perp/bridge leaves. Resume by `tk show na-2haa`.
[2026-05-06] CoinGecko API key: CG-rRAy9PuJ2yowpLftyDTk9PGJ (free tier, 10K calls/month).
[2026-05-06] DexScreener API: free, no key, used as fallback when COINGECKO_API_KEY not set.
[2026-05-06] PriceResolver: caches prices from `token_prices` + `token_metadata` ClickHouse tables.
[2026-05-06] Bridge research: `docs/BRIDGE_RESEARCH.md` (verified against Spellbook + on-chain).
[2026-05-06] Environ vars: HYPERSYNC_TOKEN, COINGECKO_API_KEY (optional), in `.env`.
[2026-05-08] Env vars: ETHERSCAN_API_KEY (Etherscan V2), DUNE_API_KEY (Dune Query API), PROTOCOL_CONTRACTS_DSN (optional Postgres for registry; default `postgresql://nexus:nexus@localhost:5434/nexus_ops`), SPELLBOOK_PATH (defaults to sibling clone).
[2026-05-08] Spellbook clone at `/home/lighto/code/avail-explorations/spellbook` (shallow ~249 MB).
[2026-05-08] Make targets: `make import-spellbook`, `make import-dune`, `make fetch-abis`.
[2026-05-11] Frontend deps: react@19, vite@8, tailwindcss@4 (`@tailwindcss/vite` + CSS-only `@theme`), recharts, @tanstack/react-query, react-router-dom, lucide-react, clsx.
[2026-05-11] API deps: fastapi, uvicorn[standard], clickhouse-connect, python-dotenv. Stateless single-shared-client; CORS wide-open in MVP.
[2026-05-11] Dune parity validator (`ops/validation/dune_parity.py`): compares canonical_events vs `dex.trades` per (chain, protocol), emits `ops/validation/runs/{date}.json`. Weekly Dagster schedule `weekly_dune_parity` (Mon 06:00 UTC, ~5 credits/run).

## Preferences

[2026-04-23] IP agreement with Avail leadership is prerequisite for all production code.
[2026-04-23] No open-source launch before Month 12. Build the product first; open-source second.
[2026-05-08] Verify framing claims with real API calls before scoping numbers in tickets/plans. The spellbook "10k+ rows" estimate was wrong by ~300×; an audit-pass mid-session caught it before further damage. Apply this pattern when sizing new external-data work.
[2026-05-11] BD MVP strategy: ship the live URL with current decoder coverage, iterate Phase B (lending/perps/staking/bridges) post-launch.
[2026-05-11] BD MVP live at https://analytics.themuse.one on shieldtx-vps. **Cloudflare Tunnel + nginx basic-auth** (originally planned LE TLS + basic-auth; ended up with CF Tunnel TLS termination + same basic-auth gate). ClickHouse + Postgres bind to 127.0.0.1 on shifted ports.

## Superseded

[2026-05-12 → 2026-05-13] ~~`token_metadata` table is empty in prod. `token_prices` asset early-returns ("No tokens...").~~ Superseded: seeded with 13 tokens via `ops/seed_token_metadata.py`. `token_prices` will run with real work; `--skip-prices` may stay in cron until throttle/cache picture stabilises.

[2026-05-13 → 2026-05-13] ~~Sink dedup pattern is the next perf bottleneck. ClickHouseSink/RawLogSink/BridgeLinkSink call `_deduplicate(events)` on every flush — chunked SELECT against existing event_ids before INSERT.~~
  Superseded: pre-insert SELECT dedup deleted in commit 431fa88. canonical_events / canonical_logs / bridge_links are ReplacingMergeTree; engine dedups at merge time. Dev migration yield: 330k events + 1.78M raw logs collapsed (prior backfill --no-dedup leftovers).

[2026-04-23 → 2026-05-13] ~~Sort key `(entity_id, timestamp)` optimizes trajectory queries.~~
  Superseded: now `(entity_id, timestamp, event_id)` so ReplacingMergeTree can dedup. The `(entity_id, timestamp)` prefix preserves the trajectory query plan.

[2026-05-06 → 2026-05-13] ~~ClickHouse `canoical_events` dedup: chunk queries at 1000 IDs max to avoid "Field value too long" HTTP error.~~
  Superseded: pre-insert dedup deleted; chunking no longer applies.

## Stale
