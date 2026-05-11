# BD MVP Roadmap

The path from "code on a laptop" to "live URL with all BD requirements
met" — broken into four phases. Each phase has an epic-level tracker
ticket; concrete work is filed as child tickets under that epic.

The MVP launches at the end of **Phase D**. Coverage / depth work
(**Phase B**) is parallelisable and continues after launch.

---

## Status snapshot

| Phase | Epic | State |
|---|---|---|
| **A — Pipeline reliability** | (none, inline) | ✅ DONE |
| **B — Coverage depth** | [na-09db](.tickets/na-09db.md) | 🟡 OPEN — 20+ child tickets parallelisable |
| **C — BD MVP (API + UI)** | [na-4yz7](.tickets/na-4yz7.md) | 🟡 C.1 done · C.2 next |
| **D — VPS deployment** | [na-7715](.tickets/na-7715.md) | 🔜 BLOCKED by C.2 |

---

## Phase A — Pipeline reliability (DONE)

Done inline May 11. Key deliverables in the repo:

- `ops/run_ingestion.py` — single-shot Dagster materialize for cron.
- `ops/run_backfill.py` — chunked historical backfill.
- `ops/CRON.md` — install + verification docs.
- `core/registry/uniswap_v4_pools.py` + `ops/backfill_uniswap_v4_pools.py`
  — UniV4 pool registry (na-w9un, na-5run, na-lh5q in flight).

---

## Phase B — Decoder + data coverage depth

**Epic: [na-09db]**

Non-blocking for MVP launch. Each child ticket can land in parallel.

### B.1 — Non-swap protocol decoders (15 leaf tickets)

The BD doc's "first action after bridge" / "second hop after swap"
queries surface these protocols once decoders are wired. Without them,
"non-swap DeFi" stays near 0.

| Protocol | Ticket | Family |
|---|---|---|
| Aave V3 | [na-0ghx] | lending |
| Lido stETH | [na-acga] | staking |
| Morpho Blue | [na-uqlq] | lending |
| Compound V3 | [na-hfyi] | lending |
| Spark | [na-fvnl] | lending |
| Fluid | [na-wif5] | lending |
| Rocket Pool rETH | [na-bws7] | staking |
| EtherFi eETH | [na-6tpb] | staking |
| Renzo ezETH | [na-2upn] | LRT |
| Kelp rsETH | [na-zxyr] | LRT |
| Pendle PT/YT/SY | [na-ngmm] | yield |
| GMX V2 | [na-qx89] | perps |
| Synthetix Perps V3 | [na-i71x] | perps |
| Vertex | [na-pzqt] | perps |
| OpenSea + Blur | [na-yshz] | NFT |

### B.2 — Pool-registry-aware DEX decoders

| Project | Ticket | Notes |
|---|---|---|
| Curve | [na-ow03] | Multiple raw topic0s; pool coins[] registry needed. |
| Balancer V2/V3 | [na-j4cx] | Vault Swap with poolId; poolId→tokens registry. |
| Long-tail (Maverick, Trader Joe, etc.) | [na-imzx] | 32 projects from `deferred_protocols.json`. |
| Broader long-tail backlog | [na-idks] | Umbrella. |

### B.3 — Bridge protocol coverage

| Ticket | Notes |
|---|---|
| [na-k7h7] | Wormhole, Mayan, CCTP, Hyperlane, Synapse, Hop, deBridge, Polygon PoS. |

### B.4 — Data depth

| Ticket | Notes |
|---|---|
| [na-t6ef] | Extend continuous backfill to 30 days. Required for BD doc 30d view. |
| [na-lh5q] | UniV4 pool registry on Arbitrum + Optimism (background, ~90 min). |

---

## Phase C — BD MVP

**Epic: [na-4yz7]**

The user-facing surface: FastAPI backend + React frontend. The BD doc
specifies "Custom UI (not Observable) for beautiful UX". Observable
remains for internal dogfooding.

### C.1 — FastAPI backend (DONE)

13 endpoints over `canonical_events`:

- `/api/bridge-flow/*` — summary, breakdown, first-action, swap-vs-non-swap,
  second-hop, activity-24h, top-protocols-after-bridge.
- `/api/spikes/*` — summary, hourly, daily, timeline, protocols.
- `/healthz`, `/api/meta/chains`.

See `api/README.md` for the full surface. Bridge-flow queries use
ClickHouse ASOF LEFT JOIN; "meaningful DeFi action" filter excludes
ERC20 transfer/approval setup noise so the BD output is about apps used,
not token plumbing.

### C.2 — React+Vite frontend ([na-xov7])

Two pages — `/bridge` and `/spikes` — per the BD doc's output table.
Stack: React + Vite + shadcn/ui + recharts + tanstack-query. Common
controls: chain filter + time window (24h / 7d / 30d). `vite build`
produces a static bundle that Phase D rsyncs to the VPS.

---

## Phase D — VPS deployment

**Epic: [na-7715]** · Rollup ticket: **[na-7pax]**

Target: Contabo VPS (`shieldtx-vps`, user `apnetv`). Ubuntu 24.04,
Docker installed.

Concrete steps (see na-7pax for the live punch list):

1. `git clone` repo on VPS at `/home/apnetv/nexus-analytics`.
2. Adapt `docker-compose.yml`: API + nginx publicly exposed; ClickHouse +
   Postgres bind to 127.0.0.1.
3. `docker compose up -d` + apply schemas.
4. Run Dune bootstrap once.
5. `nohup PYTHONPATH=. python ops/run_backfill.py --days 30` (~30h).
6. Install crontab entry (per `ops/CRON.md`).
7. nginx + Let's Encrypt for HTTPS on chosen subdomain.
8. Build frontend (`npm run build`); rsync `dist/` to `/var/www/`.
9. Lock down API CORS; basic auth or IP allowlist.

---

## Post-MVP

Once the URL is live, Phase B grinding continues. Future-state ideas
(not yet ticketed):

- Identity graph rollup view (per `core/identity/`).
- Observability dashboard for ingestion lag, sink errors, free-tier
  budget consumption.
- Self-serve segment / cohort builder.

These are out of scope until the MVP is shipped and validated with the
BD audience.

[na-09db]: ../.tickets/na-09db.md
[na-4yz7]: ../.tickets/na-4yz7.md
[na-7715]: ../.tickets/na-7715.md
[na-0ghx]: ../.tickets/na-0ghx.md
[na-acga]: ../.tickets/na-acga.md
[na-uqlq]: ../.tickets/na-uqlq.md
[na-hfyi]: ../.tickets/na-hfyi.md
[na-fvnl]: ../.tickets/na-fvnl.md
[na-wif5]: ../.tickets/na-wif5.md
[na-bws7]: ../.tickets/na-bws7.md
[na-6tpb]: ../.tickets/na-6tpb.md
[na-2upn]: ../.tickets/na-2upn.md
[na-zxyr]: ../.tickets/na-zxyr.md
[na-ngmm]: ../.tickets/na-ngmm.md
[na-qx89]: ../.tickets/na-qx89.md
[na-i71x]: ../.tickets/na-i71x.md
[na-pzqt]: ../.tickets/na-pzqt.md
[na-yshz]: ../.tickets/na-yshz.md
[na-ow03]: ../.tickets/na-ow03.md
[na-j4cx]: ../.tickets/na-j4cx.md
[na-imzx]: ../.tickets/na-imzx.md
[na-idks]: ../.tickets/na-idks.md
[na-k7h7]: ../.tickets/na-k7h7.md
[na-t6ef]: ../.tickets/na-t6ef.md
[na-lh5q]: ../.tickets/na-lh5q.md
[na-xov7]: ../.tickets/na-xov7.md
[na-7pax]: ../.tickets/na-7pax.md
