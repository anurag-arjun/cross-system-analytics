# BD Dashboard Requirements

**Source:** Colleague spec (BD/GTM use case)
**Date:** 2026-05-07

## Scope

Two dashboard views, custom UI (not Observable).

---

## 1. Bridge Flow Analytics

### Chains
Ethereum, Base, Arbitrum, Optimism, Polygon

### Timeframes
- Last 7 days
- Last 30 days

### Queries

| # | Query | Description |
|---|---|---|
| 1 | **First action after bridge** | What apps did users immediately interact with after bridging to a chain? |
| 2 | **Swap vs non-swap split** | Actions after a bridge that allow swaps vs those that don't |
| 3 | **Second hop after swap** | If they swapped on another app — what did they do after the swap? |
| 4 | **24h activity window** | What apps did users interact with in the 24 hours after bridging? |

### Outputs

- Summary KPI cards: total bridge_ins, bridge_outs, swap %, non-swap %
- Table: first action per bridge protocol (protocol, next app, action type, count, median time)
- Bar chart: swap vs non-swap split
- Table: 2nd hop after swap
- Line chart: hourly activity for 24h after bridge
- Bar chart: top protocols used after bridge (24h window)
- Table: bridge events breakdown by protocol + chain

---

## 2. Contract/App Activity (Spike Detection)

### Chains
Same as above: Ethereum, Base, Arbitrum, Optimism, Polygon

### Timeframes
- Last 24 hours
- Last 7 days

### Queries

| # | Query | Description |
|---|---|---|
| 1 | **Hourly spike detection** | Apps/contracts with >200-400% increase in hourly interactions vs 24h rolling average |
| 2 | **Daily spike detection** | Apps/contracts with >200-400% increase in daily interactions vs 7-day rolling average |

### Outputs

- Summary KPI cards: venues tracked, extreme alerts (≥400%), high alerts (≥200%), protocols tracked
- Table: extreme spikes (≥400%) — venue, protocol, chain, events, wallets, ratios
- Table: high spikes (≥200%) — same columns
- Scatter plot: events ratio vs wallets ratio, colored by alert level
- Bar chart: active protocols by venue count
- Line chart: hourly total activity timeline (7 days)

---

## Data Sources

All queries run against ClickHouse (`nexus` database), table `canonical_events`.

### Key columns used:

| Column | Used by |
|---|---|
| `entity_id` | Wallet / entity grouping |
| `event_type` | Filter: `bridge_in`, `bridge_out`, `swap`, etc. |
| `protocol` | Classification (Uniswap, Aave, Across, etc.) |
| `venue` | Specific contract address |
| `chain` | Chain filter |
| `timestamp` | Time-windowing, rolling averages |
| `token_in`, `token_out`, `amount_in_usd`, `amount_out_usd` | Volume (future enhancement) |

### Current ingestion pipeline:
- **HyperSync** → EVM adapter → ClickHouse `canonical_events`
- Chains ingesting: Ethereum, Base, Arbitrum, Optimism
- Event types decoded: swaps (DEX aggregators + direct), bridge_in, bridge_out (Across, OP Stack, Stargate/LayerZero, Arbitrum canonical)
- USD pricing: CoinGecko + DexScreener at ingestion time

### Gaps vs Requirements:

| Requirement | Status | Gap |
|---|---|---|
| Polygon ingestion | ❌ Not ingesting | HyperSync supports `polygon.hypersync.xyz`; just add chain config + RPC fallback |
| Non-swap protocol decoders | ❌ Largely missing | Schema defines `lend_*`, `perp_*`, `stake`, `lp_*`, `claim` but no decoders wired up. Port from Dune Spellbook. |
| Bridge protocol coverage | ⚠️ Partial | Have Across, Stargate/LayerZero, OP Stack, Arbitrum, Base canonical. Need Wormhole, Mayan, CCTP, Hyperlane, Synapse, Hop, deBridge DLN, Polygon PoS bridge. |
| 30-day timeframe | ⚠️ Partial | Pipeline has 7-day backfill; need 30-day window |
| Daily spike detection (7-day rolling) | ❌ Not implemented | Query exists for hourly only; need daily aggregation variant |

### Out of Scope (current phase)

- HyperEVM, MegaETH, Monad ingestion
- Gas-paid metric (interactions count is sufficient)
- Contract/app label enrichment (Etherscan / DefiLlama / OpenLabelsInitiative) — defer until decoder coverage plateaus

---

## Tech Decisions

- **Frontend:** Custom UI (not Observable) for beautiful UX
- **Backend API:** Python/FastAPI or direct ClickHouse via API routes
- **Data:** ClickHouse `canonical_events` — all queries ready, just need API layer + frontend
