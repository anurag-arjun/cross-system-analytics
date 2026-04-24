# Nexus Analytics — Facts

**Last reconciled:** 2026-04-23

## Architecture

[2026-04-23] Monorepo split: `/core` (MIT-destined), `/avail` (proprietary), `/commercial` (future enterprise tier).
[2026-04-23] Canonical event schema uses `entity_id` + `entity_type` (not wallet-specific) for horizontal compatibility.
[2026-04-23] Sort key `(entity_id, timestamp)` optimizes trajectory queries.
[2026-04-23] Schema registry in `/core/schemas/registry.yaml` — adding event types requires registry entry + adapter + test.
[2026-04-23] Trajectory primitive `trajectory(entity_id, anchor, window_before, window_after)` is the core query. Target: <2s for 30-day window.
[2026-04-23] Identity graph uses `entity_relationships` table (not flat `wallet_identity`). Supports multi-hop resolution with confidence scores.
[2026-04-23] No Iceberg in v1. ClickHouse MergeTree is primary storage. Revisit when data exceeds 1TB or multi-engine querying is needed.

## Conventions

[2026-04-23] Every engineering task tagged `[core]` or `[avail]`. No untagged tasks.
[2026-04-23] Weekly architectural guardrails run every Friday: standalone test, fake-integrator test, schema extensibility test.
[2026-04-23] Three product surfaces in priority order: (1) Nexus integrator CS tool, (2) FastBridge marketing analytics, (3) GTM/ICP scoring.
[2026-04-23] Exposure Scorecard is Phase 2 only (Months 7-9). Do not start until CS tool is in daily use and unified funnel demo is shown to FastBridge marketing.
[2026-04-23] Bridge linking: Base-only bulletproofing in Week 4-5. Arbitrum/Optimism are preview-quality. Stargate/OFT proven on Base only.

## Gotchas

[2026-04-23] HyperSync free tier is primary data source. Pocket Network public RPC is free fallback. cryo is tertiary.
[2026-04-23] No AWS S3 public datasets for Base/Arbitrum/Optimism. BigQuery only has Ethereum.
[2026-04-23] Twenty CRM deferred to Month 2+. Week 6 uses Postgres + Observable + Tooljet only.
[2026-04-23] 5 Web2 validation interviews (fintech, API company, mobile game, marketplace, B2B SaaS) are a parallel Phase 1 track. Don't skip — they determine horizontal vs. crypto-native positioning.

## Dependencies & Tooling

[2026-04-23] HyperSync URLs: eth.hypersync.xyz, base.hypersync.xyz, arbitrum.hypersync.xyz, optimism.hypersync.xyz
[2026-04-23] ClickHouse + Postgres via Docker Compose. Dagster for orchestration. Observable Framework for developer-facing analytics dashboards.
[2026-04-23] Docker Compose stack includes: ClickHouse, Postgres, Dagster, Observable Framework.
[2026-04-23] `tk` CLI for ticket tracking (stored in `.tickets/`).

## Preferences

[2026-04-23] IP agreement with Avail leadership is prerequisite for all production code.
[2026-04-23] No open-source launch before Month 12. Build the product first; open-source second.

## Superseded

## Stale
