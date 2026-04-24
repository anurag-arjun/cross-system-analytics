# Journal

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
