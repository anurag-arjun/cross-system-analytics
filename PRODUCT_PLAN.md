# Nexus Analytics — Product Plan

**Status**: draft v1  
**Last updated**: 2026-04-23  
**Horizon**: 18 months to open-source launch  
**Anchor customer**: Avail (Nexus, FastBridge, GTM engineering)  
**Long-term shape**: MIT-licensed open-source core (`/core`) + proprietary Avail layer (`/avail`) + future enterprise tier (`/commercial`)  
**Positioning**: *Analytics for teams with cross-system journeys. Crypto is our first market.*

---

## 1. The Problem

Modern products exist across multiple systems. A user journey might start in a Twitter ad, move to a landing page, continue in a mobile app, and end on-chain. Today's analytics tools are system-bound:

- **GA4** sees acquisition but not product usage.
- **PostHog** sees product but not billing.
- **Nansen** sees on-chain wallets but not off-chain identity.
- **HubSpot** sees CRM but not the actual user journey.

No tool stitches these into one view. The result: teams make decisions with partial data, optimising funnels they can see while missing the ones they can't.

This problem is universal. It shows up as:
- **Crypto**: A user discovers a bridge via Twitter, reads docs, connects a wallet, bridges, swaps, and never returns. The marketing team sees the click. The product team sees the swap. Nobody sees the drop-off between docs and wallet connect.
- **Fintech**: A user signs up, opens a brokerage account, deposits from a bank, trades, withdraws to a different bank. Current analytics sees 3–4 fragmented events. The whole journey is invisible.
- **Usage-based API**: A developer discovers you via Hacker News, reads docs, creates an API key, first call, hits rate limit, upgrades to paid. GA4 sees marketing. Mixpanel sees product. Stripe sees billing. Nobody sees the journey.
- **Gaming**: Player acquired via TikTok ad, installs app, first session, in-app purchase, invites friend, friend joins. Mobile MMPs see install. Game analytics see session. Nobody stitches them.

Our bet: the team that builds the unified layer for this problem — and makes it open-source — becomes the default for a new category.

---

## 2. The Product

### 2.1 Core primitive: the cross-systems trajectory

The atomic unit of the product is a **trajectory**: a time-ordered sequence of events for a single entity, spanning any number of source systems.

```
Entity: 0xabc...123 (wallet) / linked to ga4_client_id:xyz789

Trajectory (7 days):
  Day 0 14:32  ga4        session_start   utm_source=twitter
  Day 0 14:35  ga4        pageview        /docs/getting-started
  Day 0 14:40  posthog    pageview        /connect-wallet
  Day 0 14:41  evm_base   bridge_out      across, 0.5 ETH → Arbitrum
  Day 0 14:45  evm_arb    bridge_in       across, 0.5 ETH
  Day 0 14:47  evm_arb    swap            uniswap_v3, ETH→USDC
  Day 2 09:12  evm_arb    bridge_out      across, 400 USDC → Base
  Day 2 09:15  evm_base   bridge_in       across, 400 USDC
```

A trajectory answers questions no single tool can:
- "Which Twitter campaign drove users who actually bridged and swapped?"
- "What % of users who read the docs never connect a wallet?"
- "What do users do in the 24 hours after bridging through Integrator X?"
- "Which acquisition channel has the highest 30-day retention on-chain?"

### 2.2 Identity graph

Trajectories require identity resolution across systems. The identity graph is a first-class concept:

- **Web3 chain**: `wallet_address → ENS → Farcaster → Twitter handle`
- **Web2 chain**: `cookie → user_id → email_hash`
- **Cross-system link**: `ga4_client_id → wallet_address` (via signup linkage)

Each link has a confidence score and an expiry. The graph is queryable: "Given wallet 0xabc, what off-chain identities do we know with >0.8 confidence?"

### 2.3 Event schema

All events — EVM logs, GA4 sessions, PostHog pageviews, API calls — are normalised into one canonical table. The schema is source-agnostic:

- `entity_id` + `entity_type` (not wallet-specific)
- `event_type` + `event_category` (acquisition, product, transaction, lifecycle)
- `source_system` (evm_base, ga4, posthog, stripe)
- `link_key` for cross-system joins (depositId, session_id, guid)
- `properties` JSON, strongly typed per event_type via a schema registry

This is what makes the product horizontal. A fintech company's events (signup, deposit, trade, withdraw) fit the same schema as a crypto company's events (bridge, swap, stake, claim).

---

## 3. Product Surfaces for Avail (Phase 1, Months 1–6)

Phase 1 builds three surfaces on top of the core primitive. Each surface validates a different aspect of the platform.

### 3.1 Surface 1: Nexus Integrator CS Tool (Months 1–4)

**User**: Avail Customer Success team  
**Problem**: CS has no visibility into what happens to users after they bridge through a Nexus integrator. They don't know which integrators are healthy, which are at risk of churn, and what action to take.

**Features**:
- **Integrator directory**: Every app integrated with Nexus, tagged by tier, chain, status, lead source.
- **Per-integrator behavioural data**: Bridge volume, enabled volume, transaction types, user count, retention, error rates.
- **User trajectory view**: For a given integrator, what do users do after bridging? Bridge-only? Bridge + deposit? Bridge + swap? Delayed product usage?
- **Heuristic engine**: Rules that surface which integrators need attention:
  - `high_bridge_only_ratio` — users bridge but don't engage further
  - `outdated_sdk` — integrator hasn't upgraded to latest Nexus SDK
  - `high_error_rate` — bridge failures spiking
  - `stalled_onboarding` — signed up but no live traffic in 14 days
  - `churn_imminent` — volume declining for 3+ weeks
  - `ready_for_upgrade` — usage pattern suggests they need premium features
  - `multi_chain_opportunity` — users bridging to chains the integrator doesn't support
- **CS workflow layer**: Signals inbox, per-integrator notes, outreach tracking, integration optimisation tasks.

**Success metric**: CS team uses the tool daily. Average time-to-action on churn risk drops from 14 days to 3 days.

**Why this surface first**: It's the hardest technical problem (cross-chain trajectories, bridge stitching, integrator attribution). If you solve this, the other two surfaces are variations on the same primitives.

### 3.2 Surface 2: FastBridge Marketing Analytics (Month 5)

**User**: FastBridge marketing team  
**Problem**: Marketing sees GA4 sessions. Product sees on-chain swaps. Nobody sees the journey from tweet to transaction. Campaign attribution is guesswork.

**Features**:
- **Unified funnel view**: GA4 session → bridge event → swap event for the same entity, joined via the identity graph.
- **Campaign attribution**: "Twitter Campaign X drove 1,200 sessions, 80 wallet connects, 45 bridges, 32 swaps. CAC per swap: $Y."
- **Cohort retention**: "Users acquired via Hacker News in Week 1: 40% bridged again in Week 2, 15% in Week 4."
- **Channel comparison**: Compare Twitter, Discord, SEO, paid search by downstream on-chain value, not just clicks.

**Success metric**: A credible demo answering "Which Twitter campaign drove users who bridged to Base and swapped?" with live data. Marketing team provides written feedback.

**Why this surface matters**: It validates the horizontal thesis. If we can unify GA4 + onchain data for FastBridge, we can unify any Web2 acquisition tool + any Web3 product data. This is the proof point for Phase 2 fundraising.

### 3.3 Surface 3: GTM / ICP Scoring (Month 6)

**User**: Avail BD team  
**Problem**: BD has no systematic way to identify and prioritise prospects. They rely on manual research, Twitter rumours, and gut feel.

**Features**:
- **Wallet-level prospect ranking**: Score every wallet in the ecosystem by integration-fit (holds relevant tokens, uses similar bridges, active on target chains).
- **Segment generation**: "All wallets that bridged >$10k to Base in the last 30 days, have no Avail interaction, and hold $AVAIL."
- **Outreach tracking**: Postgres `prospects` + `outreach_log`. Observable dashboard for browsing. Tooljet admin UI for write-back (mark contacted, mark converted, notes).
- **Attribution loop**: Dagster nightly job joining `outreach_log` to `canonical_events`. Auto-flips conversion status when a prospected wallet performs a target action.

**Success metric**: Closed-loop pipeline with >50 prospects, >10 outreach attempts, and conversion attribution running.

**Why this surface last**: It relies on the identity and trajectory primitives built for Surfaces 1 and 2. It's the easiest to build once the foundations are proven.

---

## 4. Horizontal Validation (Phase 1 Parallel Track)

While building the three Avail surfaces, run a cheap validation exercise to test the horizontal thesis.

### 4.1 The 5-conversation test (Months 2–3)

Have 5 conversations with non-crypto founders/PMs about their analytics stacks. Pick specific personas:

1. A fintech PM at a neobank or brokerage
2. A PM at a usage-based API company (Stripe, Twilio, OpenAI API)
3. A growth lead at a mobile game
4. A founder of a two-sided marketplace
5. A growth/marketing lead at a B2B SaaS with a complex funnel

**The script** (don't pitch, just listen):
> "Walk me through how you understand your user journey today. Where does it fall apart?"

**Success criteria**: If 4 out of 5 describe fragmented data across 3+ tools with no unified identity or journey view, the horizontal thesis is validated. If only 1–2 do, double down on crypto.

### 4.2 Why this matters

The horizontal positioning is a 10x better company but a harder narrative. These 5 conversations (3–4 weeks, $0) will tell you whether to commit to it or stay crypto-native for longer. Don't skip this.

---

## 5. The 18-Month Arc

### Phase 1 (Months 1–6): Build for Avail, design for everyone

**Goal**: Three Avail surfaces in production, `/core` is cleanly separable, horizontal thesis validated or rejected.

**Month 1–2**: Foundation
- Canonical event schema + ingestion framework
- First two source adapters: EVM logs + GA4
- Identity graph table with first resolution walk (wallet → ENS)
- Trajectory query engine (single chain)
- Data quality monitoring scaffold

**Month 3–4**: Surface 1 (Nexus CS tool)
- Integrator directory + behavioural dashboards
- Bridge linking + cross-chain trajectory queries
- Heuristic engine + signals inbox
- CS workflow layer (notes, outreach tracking)

**Month 5**: Surface 2 (FastBridge marketing)
- GA4 + PostHog connectors
- Identity graph walk: `ga4_client_id → wallet`
- Unified funnel view + campaign attribution

**Month 6**: Surface 3 (GTM scoring) + hardening
- Wallet-level prospect ranking
- Reverse ETL to CRM (Postgres + Observable + Tooljet)
- Attribution loop
- `/core` standalone test passes
- First external-facing blog post (the problem, not the product)

**End of Phase 1 criteria**:
- Three Avail teams use the tool as their daily driver
- `/core` passes an open-source-readiness review
- 2–3 external crypto teams have seen demos and said "we want this"
- 5 Web2 interviews completed; horizontal thesis validated or rejected

### Phase 2 (Months 7–12): Harden, validate externally, build network

**Goal**: Product is polished enough for external eyes. Quiet validation with 3–5 friendly teams. Prepare for public launch.

**Track A: Harden inside Avail**
- Performance optimisations
- Documentation
- Security review
- Fix sharp edges that would embarrass externally

**Track B: External validation**
- Private beta with 3–5 friendly crypto teams (personal relationships, not paid)
- They run Docker Compose locally or you host privately
- Goal: find the 20% of code that's accidentally Avail-specific

**Track C: Prepare for public launch**
- Pick the public name (two syllables max, works in Web2 and Web3, pronounceable, .com available)
- Design landing page and docs site (Mintlify or Fumadocs)
- Production-grade Docker Compose and Helm chart
- Interactive demo on landing page
- Draft launch post (1,500–2,500 words)
- Line up 3–5 design partners willing to quote at launch

**End of Phase 2 criteria**:
- Product is used by 3+ external teams in private beta
- Zero critical bugs that would block public launch
- Launch narrative and landing page are ready
- Delaware C-corp formed, IP separation from Avail finalised

### Phase 3 (Months 13–18): Open-source launch + company formation

**Month 13: Public launch**

**Launch week**:
- Monday: Soft-launch to design partners
- Tuesday: Show HN at 8am PT. Founder stays in comments all day.
- Wednesday: Launch post across crypto and data Twitter
- Thursday: Technical deep-dive post on identity graphs for behavioural analytics
- Friday: Retrospective post, GitHub issue triage

**Success criteria**:
- 3,000–5,000 GitHub stars in the first week
- HN front page, ideally top 3
- 50+ self-hosted installs in the first month
- 10+ qualified pipeline conversations from inbound

**Month 14–15: Raise the seed**
- Target: $4–6M at $20–30M post
- Investors: Accel, Benchmark, CRV, Decibel, Felicis, Runa, GV (infra-native); Variant, Archetype, Robot Ventures, 1kx (crypto-native as partial)
- Pitch: "The Segment of behavioural data for the modern era. Built for cross-system journeys. First market is crypto — where the pain is sharpest. Roadmap is horizontal."

**Month 16–18: First revenue + hire**
- Cloud launch: Free tier (100k events/month), Pro ($149/month, 5M events), Team ($499/month, 25M events), Enterprise (custom)
- First hires: DevRel (month 13), second backend engineer (month 15), designer (month 16), first AE (month 18 if inbound justifies)
- Target: 10,000 GitHub stars, $20–40k MRR, 5 paying non-Avail customers (at least 2 non-crypto)

---

## 6. Non-Negotiable Architectural Decisions

These decisions are locked before writing production code. They determine whether the product can open-source later or is trapped.

1. **Configurable primary identity**: `entity_id` + `entity_type`, not `wallet_address`. Wallet for crypto, email for SaaS, device_id for mobile. Same code path.
2. **Pluggable event schema**: Bridges, swaps, page views, API calls are all plugins conforming to a base spec. Extensible without migrations.
3. **Source-agnostic ingestion**: EVM log, GA4 session, PostHog pageview, Stripe payment — all normalised by source-specific adapters into the same table.
4. **Identity resolution as a pipeline, not a function**: A continuously updated graph with confidence scores, not ad-hoc joins.
5. **Trajectory primitive as the core query**: `trajectory(entity_id, anchor_event, time_window)` is the foundation of every surface. Optimise for it brutally.
6. **Monorepo split**: `/core` (MIT), `/avail` (proprietary), `/commercial` (future). `/core` must pass the standalone test every week.

---

## 7. Risks

| Risk | Severity | Mitigation |
|---|---|---|
| Avail blocks open-source | Critical | IP conversation in writing before Day 1 |
| `/core` gets polluted with Avail-specific logic | High | Weekly standalone test; enforce monorepo discipline |
| Schema hardcodes wallet identity | High | `entity_id` + `entity_type` from Day 1 |
| FastBridge marketing spike fails | High | Time-box to 1 week; fallback to UTM heuristic |
| Bridge linking false matches | High | Validate weekly by sampling; maintain confidence scores |
| Horizontal thesis is wrong | Medium | 5 Web2 conversations in Months 2–3; cheap validation |
| Open-source traction is weak | Medium | Build narrative early (blog posts); line up design partners |
| Team loses focus between surfaces | Medium | Surface 1 is sacred until CS team uses it daily |
| Competitor launches first | Low | No incumbent owns this space; execution matters more than timing |

---

## 8. What We Are Not Building (Yet)

- Real-time streaming (hourly/daily is enough for v1)
- ML-based churn prediction (heuristics first)
- Automated outbound email (tool surfaces signals; humans take actions)
- Public Exposure Scorecard (Phase 2, Months 7–9)
- Twenty CRM integration (Month 2+)
- HyperEVM / Monad / MegaETH / Polygon ingestion (v2)
- Every protocol under the sun (80% coverage of top 30 is enough)

---

## 9. Next Actions

1. **Have the IP conversation with Avail leadership** (Day 1). Get agreement in writing.
2. **Set up monorepo** with `/core`, `/avail`, `/commercial` trees. Push empty structure.
3. **Draft canonical event schema design doc** (Week 1). Review with team. Freeze it.
4. **Schedule 5 Web2 interviews** (Month 2). Start reaching out now.
5. **Write first blog post** (Week 2). The problem, not the product. Publish under your name.
6. **Schedule FastBridge marketing interview** (Week 4). So the Week 5 spike has a user waiting.
