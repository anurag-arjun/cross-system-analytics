# Nexus Analytics — Engineering Plan

**Status**: draft v1  
**Last updated**: 2026-04-23  
**Aligned with**: `PRODUCT_PLAN.md` (Phase 1, Months 1–6)  
**Horizon**: 6-week sprint (Month 1 of 18-month arc)  
**Team**: 2 engineers (A = pipeline/infrastructure, B = decoders/schema) + BD consumer  
**Repository**: Monorepo with `/core` (MIT-destined), `/avail` (proprietary), `/commercial` (future)

---

## 1. Engineering Philosophy

### 1.1 `/core` is sacred

`/core` contains the generic platform: ingestion adapters, canonical schema, identity graph, trajectory engine, and headless UI components. It must be open-sourceable without modification. Every line of `/core` is reviewed against this question: *"If Avail disappeared tomorrow, would this still make sense for a fintech startup or a mobile game studio?"*

If the answer is no, it belongs in `/avail`.

### 1.2 Every task is tagged

Every engineering task is tagged `[core]` or `[avail]`. No untagged tasks exist. This makes the monorepo split enforceable in code review.

### 1.3 Build the hard thing first

The Nexus CS tool (cross-chain trajectories, bridge stitching, integrator attribution) is the hardest technical problem. Solve it first. FastBridge marketing and GTM scoring become variations on the same primitives.

### 1.4 Prove the horizontal architecture weekly

Three tests run every Friday. If any fail, fixing them is Monday's top priority:

1. **`/core` standalone test**: Fork `/core` into a throwaway directory. Run ingestion + trajectory against synthetic data without `/avail`.
2. **Fake-integrator test**: Ingest simulated Web2 events (pageviews, sessions) via a dummy adapter. Confirm the trajectory engine returns sensible sequences.
3. **Schema extensibility test**: Add a new event type to `/core/schemas/registry.yaml`. Verify ingestion picks it up without code changes outside the adapter.

---

## 2. Monorepo Structure

```
/
├── core/
│   ├── adapters/              # Source-specific ingestion adapters
│   │   ├── evm/               # cryo + HyperSync for EVM chains
│   │   ├── ga4/               # Google Analytics 4 (OAuth, read-only)
│   │   ├── posthog/           # PostHog (API key)
│   │   └── dummy/             # Simulated Web2 events for testing
│   ├── schemas/
│   │   ├── registry.yaml      # Versioned event type definitions
│   │   ├── canonical_events.sql
│   │   ├── entity_relationships.sql
│   │   └── bridge_links.sql
│   ├── identity/
│   │   ├── graph.py           # Identity graph pipeline
│   │   ├── resolvers/         # ENS, Farcaster, GA4 client_id → wallet
│   │   └── confidence.py      # Confidence scoring for links
│   ├── trajectory/
│   │   ├── engine.py          # Core trajectory query function
│   │   └── optimisations/     # ClickHouse materialised views, index tuning
│   ├── ui/                    # Headless React/Vue components
│   └── tests/
│       ├── standalone/        # /core standalone test suite
│       ├── fake_integrator/   # Simulated Web2 event tests
│       └── extensibility/     # Schema registry tests
├── avail/
│   ├── nexus_cs/
│   │   ├── integrators/       # Integrator registry + CRUD UI
│   │   ├── heuristics/        # Rules engine for CS signals
│   │   ├── signals_inbox/     # UI for CS team
│   │   └── workflows/         # Notes, outreach tracking, tasks
│   ├── fastbridge/
│   │   ├── connectors/        # GA4/PostHog config for FastBridge
│   │   ├── funnels/           # Unified funnel views
│   │   └── attribution/       # Campaign attribution logic
│   ├── gtm/
│   │   ├── scoring/           # ICP weights + prospect ranking
│   │   ├── segments/          # Segment generation rules
│   │   └── crm_sync/          # Postgres prospects + outreach_log
│   └── ops/
│       └── dagster/           # Dagster ops that wire /core into Avail
├── commercial/                # Empty for now. Future: SSO, audit, SLAs
└── ops/
    ├── docker-compose.yml     # Local dev stack
    ├── k8s/                   # Kubernetes manifests for Avail production
    └── ci/
        └── weekly_arch_tests.sh  # Automated architectural guardrails
```

---

## 3. Canonical Schema

### 3.1 Design principles

- **Source-agnostic**: EVM logs, GA4 sessions, PostHog pageviews, and Stripe payments all normalise into the same table.
- **Identity-generic**: `entity_id` + `entity_type`, not `wallet_address`. Wallet is one entity type among many.
- **Extensible without migrations**: New event types are added to `registry.yaml` + an adapter. No DDL changes.
- **Optimised for the trajectory primitive**: Sort key `(entity_id, timestamp)` makes time-windowed event sequences fast.

### 3.2 `canonical_events`

```sql
CREATE TABLE canonical_events (
  -- identity (generic, not wallet-specific)
  entity_id          String,
  entity_type        LowCardinality(String),  -- 'wallet', 'user_id', 'email_hash', 'device_id'

  -- event
  event_id           FixedString(32),         -- deterministic hash(source_system, source_event_id)
  event_type         LowCardinality(String),  -- 'swap', 'bridge_out', 'pageview', 'session_start'
  event_category     LowCardinality(String),  -- 'transaction', 'product', 'acquisition', 'lifecycle'
  timestamp          DateTime64(3),

  -- source
  source_system      LowCardinality(String),  -- 'evm_ethereum', 'evm_base', 'ga4', 'posthog'
  source_event_id    String,                  -- raw ID from source, for deduplication
  chain              LowCardinality(String),  -- NULL for Web2 sources
  block_number       Nullable(UInt64),
  block_time         Nullable(DateTime64(3)),
  tx_hash            Nullable(FixedString(32)),
  log_index          Nullable(UInt32),

  -- classification
  protocol           LowCardinality(String),
  venue              LowCardinality(String),   -- pool/market/specific contract

  -- flow (EVM-specific; nullable for Web2 events)
  token_in           Nullable(FixedString(20)),
  token_out          Nullable(FixedString(20)),
  amount_in          Nullable(Decimal(76, 0)), -- raw units, decimals in token table
  amount_out         Nullable(Decimal(76, 0)),
  amount_in_usd      Nullable(Decimal(38, 6)), -- enriched via token_prices ASOF
  amount_out_usd     Nullable(Decimal(38, 6)),

  -- counterparty / routing
  counterparty       Nullable(FixedString(20)),
  aggregator         LowCardinality(String),   -- '1inch','0x','cowswap', else ''

  -- linking (for cross-chain / cross-system stitching)
  link_key           Nullable(String),         -- e.g. depositId, guid, session_id
  link_key_type      Nullable(LowCardinality(String)),

  -- free-form
  extra              String CODEC(ZSTD(3))     -- JSON, schema-validated per event_type
) ENGINE = MergeTree
ORDER BY (entity_id, timestamp)
PARTITION BY toYYYYMM(timestamp);
```

### 3.3 `entity_relationships` (identity graph)

```sql
CREATE TABLE entity_relationships (
  from_entity        String,
  from_entity_type   LowCardinality(String),
  to_entity          String,
  to_entity_type     LowCardinality(String),
  relationship_type  LowCardinality(String),  -- 'owns', 'resolved_to', 'linked_via_ens'
  confidence_score   Float32,                 -- 0.0 - 1.0
  source             LowCardinality(String),  -- 'ens', 'farcaster', 'ga4_client_id'
  detected_at        DateTime64(3),
  expires_at         Nullable(DateTime64(3))  -- for transient links
) ENGINE = MergeTree
ORDER BY (from_entity, relationship_type, to_entity);
```

### 3.4 Schema registry

Event types are defined in `/core/schemas/registry.yaml`:

```yaml
version: 1

event_types:
  swap:
    category: transaction
    required_properties: [token_in, token_out, amount_in, amount_out]
    optional_properties: [aggregator, venue]

  bridge_out:
    category: transaction
    required_properties: [destination_chain, link_key]

  pageview:
    category: acquisition
    required_properties: [url, session_id]
    optional_properties: [referrer, utm_source, utm_medium]

  session_start:
    category: acquisition
    required_properties: [session_id, client_id]
```

**Rule**: Adding an event type requires:
1. Entry in `registry.yaml`
2. Adapter that produces canonical rows conforming to the spec
3. Unit test with synthetic data
4. No changes to `/core/trajectory/` or `/core/identity/` unless the new event type requires a new relationship type

### 3.5 EVM action vocabulary (v1)

```yaml
event_types:
  swap:           { category: transaction }
  bridge_out:     { category: transaction }
  bridge_in:      { category: transaction }
  lend_deposit:   { category: transaction }
  lend_borrow:    { category: transaction }
  lend_repay:     { category: transaction }
  lend_withdraw:  { category: transaction }
  perp_open:      { category: transaction }
  perp_close:     { category: transaction }
  perp_fill:      { category: transaction }
  lp_add:         { category: transaction }
  lp_remove:      { category: transaction }
  stake:          { category: transaction }
  unstake:        { category: transaction }
  claim:          { category: transaction }
  nft_buy:        { category: transaction }
  nft_sell:       { category: transaction }
  transfer_in:    { category: transaction }
  transfer_out:   { category: transaction }
```

**Aggregator deduplication rule**: `GROUP BY tx_hash`. If an aggregator-level event exists (1inch `OrderFilled`, 0x `RfqOrderFilled`, CoW `Trade`), it is the authoritative row; underlying DEX `Swap` logs in the same tx get `event_type = 'swap_internal'` and are excluded from trajectory queries by default.

---

## 4. Core Primitives

### 4.1 Trajectory engine

The foundational query. Given an entity + an anchor event + a time window, return the ordered event sequence.

```python
# /core/trajectory/engine.py
from datetime import timedelta
from typing import List, Optional

def trajectory(
    entity_id: str,
    anchor_event: str,           # e.g. 'bridge_out'
    window_before: timedelta,    # e.g. timedelta(days=7)
    window_after: timedelta,     # e.g. timedelta(days=7)
    filters: Optional[List[Filter]] = None,
    include_anchor: bool = True,
) -> List[CanonicalEvent]:
    """
    Return all canonical events for entity_id within:
      [anchor_timestamp - window_before, anchor_timestamp + window_after]
    
    Ordered by timestamp ascending.
    Optimised via ClickHouse ORDER BY (entity_id, timestamp).
    """
    ...
```

**Performance target**: <2 seconds for a 30-day trajectory query at Avail's data volume.

**ClickHouse optimisation**:
- Sort key `(entity_id, timestamp)` — range scan on time window is a single contiguous read.
- Partition by `toYYYYMM(timestamp)` — old partitions are cold, queries for recent windows hit only hot partitions.
- Materialised view `trajectory_index` (optional, Month 2+): pre-computed anchor-to-neighbour mappings for high-frequency queries.

### 4.2 Identity graph pipeline

A continuously running pipeline that resolves and links entities across systems.

```python
# /core/identity/graph.py
class IdentityGraph:
    def add_relationship(
        self,
        from_entity: str,
        from_type: str,
        to_entity: str,
        to_type: str,
        relationship_type: str,
        confidence: float,
        source: str,
        expires_at: Optional[datetime] = None,
    ) -> None:
        ...

    def resolve(
        self,
        entity_id: str,
        target_type: Optional[str] = None,
        min_confidence: float = 0.5,
    ) -> List[ResolvedEntity]:
        """
        Walk the identity graph from entity_id to all linked entities.
        If target_type is specified, return only entities of that type.
        Filter by min_confidence.
        """
        ...
```

**Resolution strategies** (pluggable):
- `ENSResolver`: `wallet → ens_name` via ENSNode
- `FarcasterResolver`: `wallet → fid` via Neynar
- `GA4WalletResolver`: `ga4_client_id → wallet` via FastBridge signup linkage
- `SmartWalletResolver`: `wallet → implementation_address` via `eth_getCode()` + ERC-1967

Each strategy produces a row in `entity_relationships` with a confidence score.

### 4.3 Ingestion adapters

Adapters are source-specific, stateless, and output canonical rows.

```python
# /core/adapters/base.py
class Adapter(ABC):
    @property
    @abstractmethod
    def source_system(self) -> str:
        ...

    @abstractmethod
    def ingest(self, start: datetime, end: datetime) -> Iterator[CanonicalEvent]:
        ...
```

**Adapters in v1**:

| Adapter | Source | Input | Output | Location |
|---|---|---|---|---|
| `EVMAdapter` | EVM chains | **HyperSync** primary (free tier). cryo + Pocket Network RPC fallback | `canonical_events` with `source_system = 'evm_{chain}'` | `/core/adapters/evm/` |
| `GA4Adapter` | Google Analytics 4 | OAuth API (read-only) | `canonical_events` with `source_system = 'ga4'` | `/core/adapters/ga4/` |
| `PostHogAdapter` | PostHog | API key | `canonical_events` with `source_system = 'posthog'` | `/core/adapters/posthog/` |
| `DummyWeb2Adapter` | Simulated | Generated JSON | `canonical_events` with `source_system = 'dummy'` | `/core/adapters/dummy/` |

---

## 5. Bridge Linking

Cross-chain events are joined via `link_key` + `link_key_type` in `canonical_events`.

| Bridge family | Source event | Destination event | `link_key_type` | `link_key` value |
|---|---|---|---|---|
| Across V3 | `V3FundsDeposited` | `FilledV3Relay` | `across_deposit_id` | `(originChainId, depositId)` |
| Stargate v2 (OFT) | LayerZero `PacketSent` | `PacketDelivered` | `layerzero_guid` | `guid` |
| OP Stack canonical | `DepositInitiated` / `MessagePassed` | `DepositFinalized` / `WithdrawalFinalized` | `op_withdrawal_hash` | `withdrawalHash` |
| Arbitrum Nitro | `Inbox.MessageDelivered` / `L2ToL1Tx` | `OutBoxTransactionExecuted` | `arbitrum_message_num` | `messageNum` |
| deBridge DLN | `CreatedOrder` | `FulfilledOrder` | `debridge_order_id` | `orderId` |

**Validation rule**: For every bridge family, sample 50 links weekly and confirm both sides exist in Etherscan. Maintain `link_confidence` column. False matches kill credibility.

---

## 6. Six-Week Sprint

### 6.1 Week 0 — Preconditions

**Before writing production code**:

- [ ] **[avail]** IP conversation with Avail leadership. Agreement in writing (email minimum).
- [ ] **[core]** Monorepo scaffold: `/core`, `/avail`, `/commercial` trees pushed.
- [ ] **[core]** Canonical event schema design doc reviewed and frozen.

### 6.2 Week 1 — Ingestion + Trending MVP (Ethereum only)

**Theme**: Prove the pipeline works. BD gets value on Friday.

**[core]** Set up HyperSync client for Ethereum. Pull 30d of blocks + logs + transactions. Validate row counts vs. Etherscan.  
**[core]** `tx_hourly` materialised view. First trending query: `COUNT / LAG(COUNT, 24h) > 3` windowed by `(chain, to_address)`. Output to Postgres mart.  
**[core]** Write canonical event schema spec (this doc, §3). Define `entity_id` + `entity_type`, registry format, first 15–20 EVM event types.  
**[core]** Benchmark HyperSync vs. cryo + RPC for 1,000 blocks. Document latency and throughput.  
**[avail]** Contract label loader — parse DefiLlama adapters into `contract_labels`.  
**[avail]** Seed `protocol_registry` with top 30 addresses.

**Weekly tests**: Run §1.4 architectural guardrails.

**Deliverable**: BD has a working ETH trending dashboard.

### 6.3 Week 2 — Trending across 4 chains + DEX prep

**Theme**: Expand coverage. First canonical events land.

**[core]** Add Base + Arbitrum + Optimism via HyperSync (all 3 chains on free tier). Pull 30d blocks + logs for each.  
**[core]** Soda checks on `tx_hourly` freshness + row counts.  
**[core]** Clone Spellbook, identify `dex_trades` dependencies, write ClickHouse port plan.  
**[core]** Hand-write first three decoders: Across `V3FundsDeposited`, Across `FilledV3Relay`, Uniswap V3 `Swap`.  
**[avail]** First canonical-event rows in ClickHouse.

**Weekly tests**: Run §1.4 architectural guardrails.

**Deliverable**: 4-chain trending dashboard.

### 6.4 Week 3 — DEX fork + Bridge linking (ETH↔Base via Across)

**Theme**: Trajectory primitive works for same-chain queries.

**[core]** Port Spellbook `dex_trades` to ClickHouse. Spot-check 50 swaps against Dune.  
**[core]** Port Aave V3 + Compound lending spells. Hand-write Morpho decoder.  
**[core]** `bridge_links` table. Join `V3FundsDeposited` → `FilledV3Relay` via `(originChainId, depositId)`. Validate 50 samples.  
**[core]** Trajectory primitive v1: `trajectory(entity_id, anchor, window_before, window_after)`. Same-chain only.

**Weekly tests**: Run §1.4 architectural guardrails.

**Deliverable**: Same-chain trajectory queries work.

### 6.5 Week 4 — Cross-chain trajectories + Identity graph v1

**Theme**: Cross-chain journeys work. First real BD output.

**[core]** Cross-chain trajectory: `bridge_out` on chain A + matched `bridge_in` on chain B + next-N events on B.  
**[core]** BD-facing queries: post-bridge apps, 2nd-hop swap rate, "bridged and idle" rate.  
**[core]** Extend decoders: Moonwell, friend.tech-era apps, Uniswap V4 hooks.  
**[core]** `entity_relationships` graph table. ENSNode deployed, nightly ENS + Basenames materialisation.

**Weekly tests**: Run §1.4 architectural guardrails.

**Deliverable**: First bridge-flow CSV to BD. ENS coverage for active wallets.

### 6.6 Week 5 — Cross-systems journey tracking (primary) + Onchain expansion (preview)

**Theme**: Prove the unified-journey thesis. This is the most important week for the 18-month arc.

**[core]** GA4 OAuth adapter (read-only) → ingest `session_start`, `pageview` into `canonical_events`.  
**[core]** PostHog adapter (optional if time) → ingest `pageview`, `$autocapture`.  
**[core]** `DummyWeb2Adapter` → generate simulated pageviews + sessions for the fake-integrator test.  
**[avail]** Identity graph walk: `ga4_client_id → wallet` via FastBridge signup linkage.  
**[avail]** Unified funnel view: GA4 session → bridge event → swap event for the same entity.  
**[core]** Preview: ETH → Arbitrum + Optimism via Across. Reverse direction (L2→L1) as preview.  
**[core]** Preview: LayerZero `guid` linking for Stargate + OFT on Base only.

**Weekly tests**: Run §1.4 architectural guardrails. **This week the fake-integrator test must pass with real Web2 data.**

**Deliverable**: A credible unified-funnel demo (GA4 → bridge → swap) for FastBridge marketing.

### 6.7 Week 6 — Activation + Polish

**Theme**: Close the loop. Three surfaces demoed.

**[avail]** Polish unified funnel view. Show to FastBridge marketing. Record feedback.  
**[avail]** Postgres `prospects` + `outreach_log`. Observable dashboard. Tooljet admin UI.  
**[avail]** Attribution loop: Dagster nightly job joining `outreach_log` to `canonical_events`.  
**[core]** Fix top 3 schema gaps from GA4 adapter.  
**[core]** Fix top 3 decoder gaps from BD. Add DeBridge DLN order API.

**Weekly tests**: Run §1.4 architectural guardrails.

**Deliverable**: Closed-loop GTM pipeline. BD measures conversions. FastBridge marketing has seen the demo.

---

## 7. End-of-Sprint Engineering Criteria

By end of Week 6, the following must be true:

1. **[avail] Nexus CS surface**: CS team has integrator dashboard with cross-chain trajectories. ≥1 churn-risk signal surfaced and acted upon.
2. **[avail] FastBridge surface**: Marketing team has seen live unified-funnel demo (GA4 → bridge → swap) and provided written feedback.
3. **[avail] GTM surface**: BD has closed-loop pipeline with >50 prospects, >10 outreach attempts, conversion attribution running.
4. **[core] Standalone test passes**: `/core` forks cleanly, runs against synthetic data, executes ingestion + trajectory without `/avail`.
5. **[core] Fake-integrator test passes**: Simulated Web2 event stream (pageviews + sessions) ingested, normalised, queried via trajectory engine.
6. **[core] Schema extensibility test passes**: New event type added to `registry.yaml`, picked up by ingestion without adapter-external code changes.
7. **[core] Performance target**: 30-day trajectory query runs in <2 seconds at Avail's data volume.
8. **[core] Identity graph**: `wallet → ENS` resolution works. Schema supports `cookie → user_id → email` without migration.

If any of 1–8 are false, the sprint is not complete.

---

## 8. OSS Components

| Component | Role | Decision | License | Status |
|---|---|---|---|---|
| **Envio HyperSync** | **Primary data source for all 4 chains** | **Adopt** | **Free tier** | **Very active** |
| `paradigmxyz/cryo` | Fallback backfill to Parquet via RPC | Adopt | Apache-2.0 | Maintained |
| Pocket Network | Free public RPC fallback | Adopt | Free | Active |
| `duneanalytics/spellbook` | DEX/lending SQL | Fork | Apache-2.0 | Very active |
| `DefiLlama/DefiLlama-Adapters` | Protocol address map | Adopt | MIT | Very active |
| `namehash/ensnode` | ENS indexer | Self-host | MIT | Active |
| Neynar API | Farcaster identity | Adopt | Commercial (free tier) | Active |
| TRM Labs + `0xB10C/ofac-sanctioned-digital-currency-addresses` | Sanctions | Adopt | MIT (mirror) | Active |
| Observable Framework | Developer-facing analytics dashboards | Adopt | ISC | In stack |
| Tooljet / Appsmith | Admin UI | Adopt | AGPL / Apache-2.0 | Production-ready |
| Twenty | CRM (Month 2+) | Adopt if needed | AGPL | Pre-1.0 |

---

## 9. Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Spellbook port takes >2 weeks | Medium | Medium | Fallback: Dune API mirror |
| HyperSync free tier limits exceeded | Low | High | Pocket Network public RPC (free) → cryo + Chainstack free tier (3M req/mo) |
| Bridge linking false matches | Medium | High | Weekly sampling; `link_confidence` column |
| Smart-wallet false positives | Medium | Medium | Flag in graph; downrank in BD segments |
| `/core` polluted with Avail logic | High | High | Weekly standalone test; strict code review |
| Schema hardcodes wallet identity | Low (mitigated) | High | `entity_id` + `entity_type` from Day 1 |
| FastBridge GA4 → wallet mapping missing | Medium | High | Fallback: UTM + time-based heuristic |
| IP conversation blocked | Medium | Critical | Week 0 precondition; no code before agreement |

---

## 10. What We Are Not Building (Yet)

- Real-time streaming (hourly/daily is enough)
- ML-based churn prediction (heuristics first)
- Automated outbound email (humans take actions)
- Public Exposure Scorecard (Phase 2, Months 7–9)
- Twenty CRM integration (Month 2+)
- HyperEVM / Monad / MegaETH / Polygon (v2)
- Custom indexers where Spellbook/DefiLlama cover it
- Twitter/X wallet resolver (enrich-by-join only)
- Every protocol under the sun (top 30 covers 80%)

---

## 11. Next Actions

1. [ ] **[avail]** Have IP conversation with Avail leadership. Get written agreement.
2. [ ] **[core]** Set up monorepo. Push empty `/core`, `/avail`, `/commercial` trees.
3. [ ] **[core]** Draft canonical schema design doc. Review with team. Freeze.
4. [ ] **[core]** Set up ClickHouse cluster + Postgres.
5. [ ] **[core]** Set up Dagster for orchestration.
6. [ ] **[core]** Write first blog post (the problem, not the product). Publish under your name.
7. [ ] **[avail]** Schedule FastBridge marketing interview for Week 4.
8. [ ] **[avail]** Start reaching out for 5 Web2 interviews (Month 2–3).
