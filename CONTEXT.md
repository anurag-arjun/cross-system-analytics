# Nexus Analytics — Domain Context

**Status**: v1 — captured during grill-with-docs session  
**Last updated**: 2026-04-29

---

## Glossary

### Core Entities

| Term | Definition |
|---|---|
| **Canonical Event** | A normalized row in `canonical_events` representing one user action (swap, bridge, pageview, etc.) with a uniform schema across all data sources. |
| **Entity** | A participant in an event. Identified by `(entity_id, entity_type)`. `entity_type` values: `wallet`, `user_id`, `email_hash`, `device_id`. A wallet address is just one entity type. |
| **Bridge Link** | A resolved cross-chain connection between a `bridge_out` event on a source chain and a `bridge_in` event on a destination chain. Stored in `bridge_links`. |
| **Pending Bridge Out** | A `bridge_out` event that has been observed but not yet matched to its corresponding `bridge_in`. Tracked in Postgres `pending_bridge_outs` table. |
| **Trajectory** | A time-ordered sequence of canonical events for a given entity (or group of entities) within a time window around an anchor event. |
| **Anchor Event** | The focal event of a trajectory query. E.g., "first Aerodrome swap" or "bridge_out to Base." The trajectory engine looks before and after this anchor. |
| **Protocol** | A DeFi protocol, bridge, or application. E.g., `uniswap_v3`, `aerodrome`, `across`, `aave_v3`. Protocols have one or more contract addresses per chain. |
| **Venue** | A specific contract instance within a protocol. E.g., a specific Uniswap V3 pool, an Aave V3 market, an Across SpokePool. |
| **Aggregator** | A DEX aggregator that routes through underlying DEXs. E.g., `1inch`, `0x`, `cowswap`. Its event is authoritative for a tx; underlying DEX `Swap` events in the same tx are `swap_internal`. |
| **Source System** | The origin of an event. Format: `evm_<chain>` for on-chain, `ga4`, `posthog` for Web2. |

### Bridge Families

| Term | Definition |
|---|---|
| **Intent Bridge** | A bridge where a solver or relayer fronts liquidity on the destination chain. User funds are locked on source; solver delivers equivalent funds on destination. Matching key is an order/deposit ID. Examples: Across, deBridge DLN, Hop. |
| **Message-Passing Bridge** | A bridge that relays a message from source to destination chain, with token transfer as part of the message payload. Matching key is a message nonce or GUID. Examples: LayerZero, Wormhole, Axelar. |
| **Canonical Bridge** | The native bridge built into an L2's architecture. Funds are locked in an L1 escrow contract and minted on L2 (or burned on L2 and released on L1). Matching is via withdrawal hash or message nonce. Examples: OP Stack, Arbitrum Nitro. |
| **Endpoint ID (eid)** | LayerZero's chain identifier. NOT the same as `chain_id`. E.g., Ethereum mainnet = 30101, Base = 30184. Required for Stargate/LayerZero event matching. |

Full EID mapping:
```
ethereum:  eid=30101  chain_id=1
base:      eid=30184  chain_id=8453
arbitrum:  eid=30110  chain_id=42161
optimism:  eid=30111  chain_id=10
polygon:   eid=30109  chain_id=137
avalanche: eid=30106  chain_id=43114
```

### Query Patterns

| Term | Definition |
|---|---|
| **Omnichain Wallet Timeline** | Trajectory query that fetches all events for a wallet across ALL chains in a time window, ordered by timestamp. Does not require bridge linking. |
| **Bridge-Following Timeline** | Trajectory query that starts from a `bridge_out`, resolves the `bridge_link` to the destination chain, and includes events on the destination chain after the `bridge_in`. Requires working bridge linking. |
| **Protocol-Aware Anchor** | An anchor event specified by both `event_type` AND `protocol`. E.g., `event_type="swap" AND protocol="aerodrome"`. |
| **First-Interaction Anchor** | An anchor event found via `MIN(timestamp)` for a given `(entity_id, protocol, event_type)` combination, not `MAX(timestamp)`. |

---

## Architecture Decisions

### ADR-001: Persistent Pending Bridge Table

**Status**: Agreed, not yet implemented  
**Decision**: Bridge_out events are written immediately to a Postgres `pending_bridge_outs` table when decoded. The matching job queries unmatched pending rows and attempts to find corresponding bridge_in events. When matched, the bridge_links table is updated and the pending row is marked `matched = true`.

**Rationale**: Bridge latencies vary from minutes to 7 days. In-memory state (the current `BridgeLinkEngine` Python dict) is lost on process exit, making cross-batch matching impossible.

**Consequences**: Adds Postgres dependency for operational state. Requires retry scheduling with bridge-family-specific cadences.

### ADR-002: Precise Matching for Canonical Bridges

**Status**: Agreed, not yet implemented  
**Decision**: OP Stack and Arbitrum L2→L1 withdrawals use precise matching via `withdrawalHash` (OP Stack) and `OutBoxTransactionExecuted` (Arbitrum), not `(from, to, amount)` heuristics.

**Rationale**: 7-day latency creates high risk of false positives with heuristic matching. Withdrawals are high-value events worth getting right.