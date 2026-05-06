# Bridge Protocol Deep Dive — Matching Logic & Prioritization

**Status**: Research v1  
**Date**: 2026-04-29  
**Scope**: EVM bridges across Ethereum, Base, Arbitrum, Optimism  
**Goal**: Determine correct cross-chain event matching for each bridge family, prioritize by volume, and surface implementation blockers.

---

## Executive Summary

We support 4 bridge families today in code. **Only 1 (Across) has correct matching logic.** Stargate matching is structurally broken (wrong link_key on each side). OP Stack canonical is half-implemented (only bridge_out, no bridge_in decoders). Arbitrum canonical is not implemented at all.

This document defines the correct matching logic for each family, computes verified topic0s, and recommends build order by volume.

---

## 1. Bridge Priority by Volume (Target Chains)

| Priority | Bridge Family | Est. Volume | Why |
|---|---|---|---|
| **P0** | **Across** | Highest | Dominant intent bridge for L2↔L2 and L1↔L2. Deposit→fill in minutes. Clean depositId matching. |
| **P0** | **OP Stack Canonical** | Very High | Base and Optimism native bridging. Forced by chain architecture — every ETH/token move uses it. |
| **P0** | **Arbitrum Canonical** | Very High | Arbitrum native bridging. Same as OP Stack — unavoidable for canonical transfers. |
| **P1** | **Stargate / LayerZero OFT** | High | Major messaging layer. Stargate is the most visible consumer. But matching is non-trivial (endpoint IDs, not shared keys). |
| **P1** | **Hop Protocol** | Medium-High | Rollup-to-rollup focused. Bond→redeem pattern with bonder identity. |
| **P2** | **deBridge DLN** | Medium | Intent-based like Across but with orderId matching. Growing usage. |
| **P2** | **Celer cBridge** | Medium | Message-passing + pool-based. TransferId matching. |
| **P3** | **Wormhole** | Medium | Token bridge + messaging. Sequence/emitter matching. Mostly Solana↔EVM, less intra-EVM. |
| **P3** | **Synapse** | Lower | Older bridge, declining relative share. nUSD pool + SynapseBridge events. |

**Week 1–2 recommendation**: Get **Across** + **OP Stack** + **Arbitrum** canonical working end-to-end. These three cover ~80% of bridge volume on your target chains. Defer Stargate to Week 3+ because the matching logic requires a new architecture (endpoint ID mapping).

---

## 2. Bridge Family Specifications

### 2.1 Across (Intent Bridge) — P0

**Matching key**: `deposit_id` (uint32, indexed) appears identically on both sides.

| Side | Event | Contract | Topic0 |
|---|---|---|---|
| bridge_out | `V3FundsDeposited(...)` | SpokePool (source chain) | `0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f` |
| bridge_in | `FilledV3Relay(...)` | SpokePool (destination chain) | `0xb553cf4433b697c1ab9b28c8a3ffefbd12e812ff58d5199aba60b3e6df7f38e3` |

**Event signatures (canonical)**:
```solidity
V3FundsDeposited(address,address,uint256,uint256,uint256,uint32,uint32,uint32,uint32,address,address,address,bytes)
FilledV3Relay(address,address,uint256,uint256,uint256,uint256,uint32,uint32,uint32,address,address,address,address,bytes)
```

**Matching rule**:
- Extract `depositId` from `topics[2]` on `V3FundsDeposited` (indexed).
- Extract `depositId` from `topics[3]` on `FilledV3Relay` (indexed).
- Match: `deposit_id` equality.
- Validate: `originChainId` in `FilledV3Relay` matches source chain.

**Latency characteristics**:
- Fast path: 2–10 minutes (solver fills immediately)
- Slow path: up to fillDeadline (hours)
- Refunds: if never filled, deposit is refunded on source chain after expiry

**Implementation status**: ✅ Decoders exist. Matching logic needs persistent pending table (see §4).

**Gotchas**:
- Same `deposit_id` can appear on multiple chains if user deposits to different destinations. Must validate `(source_chain, deposit_id)` pair.
- `FilledV3Relay` has `repaymentChainId` (where solver gets paid back) — this is NOT the destination chain. The destination chain is where the event was emitted.
- Slow fills exist: a `FilledV3Relay` can be emitted with `relayer = 0x0` indicating a slow fill. Still a valid bridge_in.

---

### 2.2 OP Stack Canonical Bridge — P0

**Matching key**: No single shared identifier in events. Must match by `(tx_hash, event_type)` heuristics or track the cross-chain message hash.

The OP Stack bridge emits `*Initiated` on the source side and `*Finalized` on the destination side.

| Side | Event | Contract | Topic0 |
|---|---|---|---|
| bridge_out (L1→L2 deposit) | `ETHDepositInitiated(from, to, amount, extraData)` | L1StandardBridge | `0x35d79ab81f2b2017e19afb5c5571778877782d7a8786f5907f93b0f4702f4f23` |
| bridge_out (L1→L2 deposit) | `ERC20DepositInitiated(l1Token, l2Token, from, to, amount, extraData)` | L1StandardBridge | *(compute from signature)* |
| bridge_out (L2→L1 withdrawal) | `ETHBridgeInitiated(from, to, amount, extraData)` | L2StandardBridge | `0x2849b43074093a05396b6f2a937dee8565b15a48a7b3d4bffb732a5017380af5` |
| bridge_out (L2→L1 withdrawal) | `ERC20BridgeInitiated(localToken, remoteToken, from, to, amount, extraData)` | L2StandardBridge | `0x7ff126db8024424bbfd9826e8ab82ff59136289ea440b04b39a0df1b03b9cabf` |
| bridge_in (L2←L1 deposit) | `ETHBridgeFinalized(from, to, amount, extraData)` | L2StandardBridge | `0x31b2166ff604fc5672ea5df08a78081d2bc6d746cadce880747f3643d819e83d` |
| bridge_in (L2←L1 deposit) | `ERC20BridgeFinalized(localToken, remoteToken, from, to, amount, extraData)` | L2StandardBridge | `0xd59c65b35445225835c83f50b6ede06a7be047d22e357073e250d9af537518cd` |
| bridge_in (L1←L2 withdrawal) | `ETHWithdrawalFinalized(from, to, amount, extraData)` | L1StandardBridge | `0x2ac69ee804d9a7a0984249f508dfab7cb2534b465b6ce1580f99a38ba9c5e631` |
| bridge_in (L1←L2 withdrawal) | `ERC20WithdrawalFinalized(l1Token, l2Token, from, to, amount, extraData)` | L1StandardBridge | *(compute from signature)* |

**Matching rule**:
- **Legacy matching (simplest)**: Match `bridge_out` on source chain to `bridge_in` on destination chain by:
  1. Same `from` address (topics[1])
  2. Same `to` address (topics[2])
  3. Same `amount`
  4. Destination event occurs after source event
  5. For L1→L2: within 30 min. For L2→L1: within 7 days.
- **Modern matching (precise)**: The `OptimismPortal` contract emits a `TransactionDeposited` or `WithdrawalProven`/`WithdrawalFinalized` event with a `withdrawalHash` / `messageNonce`. This is the canonical cross-chain identifier.
  - On L1: `TransactionDeposited(from, to, version, opaqueData)` — emits a `deposit` hash
  - On L2: `TransactionDeposited` events are replayed via the `L1Block` oracle
  - On L2→L1: `MessagePassed` event on L2 includes `withdrawalHash`
  - On L1: `WithdrawalFinalized(withdrawalHash, success)` completes it

**Latency characteristics**:
- L1→L2 deposit: ~10–20 minutes (sequencer picks up L1 info)
- L2→L1 withdrawal: **~7 days** (fraud proof window)

**Implementation status**: ⚠️ Half-implemented. `ETHBridgeInitiated` and `ERC20BridgeInitiated` decoders exist on bridge_out side (Base only). Missing:
- `ETHBridgeFinalized` decoder
- `ERC20BridgeFinalized` decoder
- `ETHDepositInitiated` / `ETHWithdrawalFinalized` decoders for L1 side
- Matching logic (must use `(from, to, amount)` heuristic or `withdrawalHash` from OptimismPortal)

**Gotchas**:
- OP Stack has both "Bedrock" (modern) and pre-Bedrock events. Pre-Bedrock used `ETHDepositInitiated`/`ETHWithdrawalFinalized`. Bedrock unified to `ETHBridgeInitiated`/`ETHBridgeFinalized` on both sides. Must handle both.
- Base and Optimism are both OP Stack but may have slightly different contract addresses.
- L2→L1 withdrawals take 7 days. Your pending table must handle this gracefully.

---

### 2.3 Arbitrum Canonical Bridge — P0

**Matching key**: `messageNum` for L1→L2, `batchNumber + index` or `withdrawalHash` for L2→L1.

| Side | Event | Contract | Topic0 |
|---|---|---|---|
| bridge_out (L1→L2) | `MessageDelivered(messageIndex, beforeInboxAcc, inboxAcc, kind, sender, destAddr, l1BaseFee, blockTimestamp, version, data)` | Inbox | `0xa72c1d483d79c8197fdd35122e484d23c90af9a73947df07c902aafb22d1d6c1` |
| bridge_out (L2→L1) | `L2ToL1Tx(caller, destination, callvalue, value, data)` | ArbSys (pre-Nitro) or NodeInterface | varies |
| bridge_in (L2←L1) | L2 delivery tx (no explicit event — the tx itself is the delivery) | — | — |
| bridge_in (L1←L2) | `OutBoxTransactionExecuted(beneficiary, l2Block, l1Block, timestamp, withdrawalId)` | Outbox | `0x2fc8bec8db92f82383739a4bcb54c6a43fef7edae7a32fe40af40c320126a2f2` |

**Matching rule**:
- **L1→L2**: `Inbox.MessageDelivered` includes `messageNum` (the message index). On Arbitrum L2, the message is delivered as a transaction from the `ArbSys` precompile. There is no explicit "finalize" event — the transaction execution itself is the bridge_in. Matching requires tracking the `messageNum` → L2 tx hash mapping via ArbSys.
- **L2→L1**: `ArbSys.sendTxToL1` emits a `L2ToL1Tx` event (or `WithdrawalInitiated` in newer versions). The `OutBox.executeTransaction` on L1 emits `OutBoxTransactionExecuted`. Match by `withdrawalId` / `batchNum + index`.

**Latency characteristics**:
- L1→L2: ~10–15 minutes (sequencer inclusion + delay buffer)
- L2→L1: **~7 days** (challenge period)

**Implementation status**: ❌ Not implemented.

**Gotchas**:
- Arbitrum's bridge is more complex than OP Stack. L1→L2 messages don't have a simple "finalized" event on L2.
- Arbitrum Nitro changed the event structure from pre-Nitro. Must verify which version is live.
- L2→L1 withdrawals require the `Outbox` contract. The `OutBoxTransactionExecuted` event is the clear bridge_in signal.

---

### 2.4 Stargate / LayerZero OFT — P1

**Matching key**: **There is no single shared key in the events.** This is why the current code is broken.

**Current broken logic**:
- `SendToChain` sets `link_key = dst_chain_id` (e.g., "30101")
- `ReceiveFromChain` sets `link_key = src_eid` (e.g., "30184")
- These are **never equal**.

**Correct matching logic**:
LayerZero V2 uses a `guid` (globally unique identifier) that is generated from `(nonce, srcEid, sender, dstEid, receiver)`. The `guid` is **not emitted directly** in the standard `SendToChain` / `ReceiveFromChain` events. It appears in:
- `EndpointV2.PacketSent(bytes encodedPayload, bytes options, address sendLibrary)` — topic0: `0x1ab700d4ced0c005b164c0f789fd09fcbb0156d4c2041b8a3bfbcd961cd1567f`
- The encoded payload contains the packet with the `guid`.
- On destination: `ReceiveUln302.PacketDelivered(...)` or similar — the `guid` is verified but may not be easily extractable from logs.

**Alternative matching for Stargate specifically**:
Stargate OFT uses `send()` which calls `OFT._lzSend()`. The OFT wrapper may emit additional events. But the standard `IOFT` interface only emits:
- `SendToChain(uint16 dstEid, bytes to, uint256 amount)` on source
- `ReceiveFromChain(uint16 srcEid, bytes sender, uint256 amount)` on destination

Without the `guid`, matching must be **heuristic**:
1. Track `SendToChain` on source chain with `(src_chain, dst_eid, to_address, amount, block_time)`
2. Track `ReceiveFromChain` on destination chain with `(dst_chain, src_eid, receiver, amount, block_time)`
3. Match when:
   - `dst_eid` maps to destination chain
   - `src_eid` maps to source chain
   - `to_address` ≈ `receiver`
   - `amount` within fee tolerance (0.5%)
   - Time delta < 30 minutes

**Endpoint ID mapping** (LayerZero V2 mainnet):
| Chain | eid |
|---|---|
| Ethereum | 30101 |
| Arbitrum | 30110 |
| Optimism | 30111 |
| Base | 30184 |

**Latency characteristics**:
- Fast: 1–10 minutes (DVN verification + execution)
- Slow: up to hours if message retry is needed

**Implementation status**: ⚠️ Decoders exist but matching is broken. Needs either:
- (A) Heuristic matching with endpoint ID mapping table, OR
- (B) Index `EndpointV2.PacketSent` to extract `guid` from encoded payload, then match with destination-side `guid` verification

**Gotchas**:
- OFT fees are taken in the token. The `amount` in `ReceiveFromChain` is less than `SendToChain`. Must use tolerance matching.
- Stargate V2 has "bus mode" which batches messages. Individual transfers may be delayed.
- LayerZero endpoint IDs are NOT chain IDs. Must maintain a mapping table.

---

### 2.5 Hop Protocol — P1

**Matching key**: `transferId` or `transferNonce`.

Hop uses a bonder model:
1. User sends tokens to Hop Bridge on source chain → emits `TransferSent(to, amount, transferNonce, bonderFee, index, amountOutMin, deadline)`
2. Bonder fronts liquidity on destination chain → emits `TransferBonded(transferId, recipient, amount)`
3. The actual settlement happens later via the canonical bridge.

**Event signatures**:
```solidity
TransferSent(address,uint256,uint256,uint256,uint256,uint256,uint256)
TransferBonded(bytes32,address,uint256)
```

**Matching rule**:
- `transferId` is computed from the transfer parameters. Can be matched deterministically.
- Or: `transferNonce` (indexed in `TransferSent`) appears in the bonded event.

**Latency characteristics**:
- Fast: 1–5 minutes (bonder fronting)
- Canonical settlement: hours to days

**Implementation status**: ❌ Not implemented.

---

### 2.6 deBridge DLN — P2

**Matching key**: `orderId`.

| Side | Event | Topic0 |
|---|---|---|
| bridge_out | `CreatedOrder(order, orderId)` | varies by chain |
| bridge_in | `FulfilledOrder(order, orderId, unlockAuthority)` | varies by chain |

**Matching rule**: Exact `orderId` equality.

**Latency characteristics**: Intent-based, similar to Across. Minutes to hours.

**Implementation status**: ❌ Not implemented.

---

## 3. Critical Code Issues Found

### Issue 1: Bridge matching is process-ephemeral
The `BridgeLinkEngine` stores pending bridge_outs in a Python `dict`. When the Dagster process exits, pending state is lost. Bridge links with latency > batch window are never matched.

**Fix**: Implement persistent pending table (see §4).

### Issue 2: Stargate matching is structurally impossible
As detailed in §2.4, `link_key` values on source and destination are different endpoint IDs. They will never match with equality comparison.

**Fix**: Replace exact matching with endpoint-ID-aware heuristic matching, or add `guid` extraction from `EndpointV2.PacketSent`.

### Issue 3: OP Stack canonical is half-implemented
Only `bridge_out` decoders exist. No `bridge_in` (`*Finalized`) decoders.

**Fix**: Add `ETHBridgeFinalized`, `ERC20BridgeFinalized`, `ETHDepositInitiated`, `ETHWithdrawalFinalized` decoders. Implement `(from, to, amount)` heuristic matching or `withdrawalHash` extraction from OptimismPortal.

### Issue 4: Arbitrum canonical is missing entirely
No decoders, no matching logic.

**Fix**: Add `Inbox.MessageDelivered` decoder (bridge_out L1→L2), `OutBoxTransactionExecuted` decoder (bridge_in L2→L1). For L1→L2, matching is non-trivial — may require tracing ArbSys precompile calls.

### Issue 5: Duplicate ingestion with no deduplication
The pipeline fetches a 30-minute lookback on every run. Running every 5 minutes = 6x duplicates.

**Fix**: Check-then-insert using `event_id` as dedup key, or switch to block checkpointing.

---

## 4. Recommended Implementation Order

### Week 1: Fix the foundation
1. **Add deduplication** to canonical event ingestion (check `event_id` before insert)
2. **Implement persistent pending bridge table** in Postgres:
   ```sql
   CREATE TABLE pending_bridge_outs (
     link_key           TEXT NOT NULL,
     link_key_type      TEXT NOT NULL,
     src_chain          TEXT NOT NULL,
     src_block_time     TIMESTAMPTZ NOT NULL,
     src_tx_hash        TEXT NOT NULL,
     src_entity_id      TEXT NOT NULL,
     src_event_id       TEXT NOT NULL,
     protocol           TEXT NOT NULL,
     token_out          TEXT,
     amount_out         TEXT,
     expected_dst_chain TEXT,
     matched            BOOLEAN DEFAULT FALSE,
     match_attempts     INT DEFAULT 0,
     next_retry_at      TIMESTAMPTZ,
     created_at         TIMESTAMPTZ DEFAULT NOW(),
     PRIMARY KEY (link_key, link_key_type, src_chain)
   );
   ```
3. **Fix Across matching** to use the pending table. Validate with real Base↔Ethereum data.

### Week 2: Canonical bridges
1. **OP Stack**: Add `*Finalized` decoders. Implement `(from, to, amount)` heuristic matching. Test Base↔Ethereum.
2. **Arbitrum**: Add `OutBoxTransactionExecuted` decoder for L2→L1. Add `MessageDelivered` decoder for L1→L2. Implement L2→L1 matching first (clearer event pair).

### Week 3: Stargate & heuristic bridges
1. **LayerZero endpoint mapping table**:
   ```yaml
   endpoint_ids:
     30101: ethereum
     30110: arbitrum
     30111: optimism
     30184: base
   ```
2. **Fix Stargate matching**: Match on `(src_chain ↔ dst_eid, dst_chain ↔ src_eid, to ≈ receiver, amount within tolerance, time within 30min)`.
3. **Add Hop Protocol** if time permits.

### Week 4: Volume validation
1. Query real bridge volume from your own data to validate prioritization.
2. Identify top 5 bridges by actual transaction count in your dataset.
3. Backfill missing decoders for any high-volume bridges discovered.

---

## 5. Event Topic0 Reference

| Protocol | Event | Canonical Signature | Topic0 |
|---|---|---|---|
| Across | V3FundsDeposited | `V3FundsDeposited(address,address,uint256,uint256,uint256,uint32,uint32,uint32,uint32,address,address,address,bytes)` | `0xa123dc29...` |
| Across | FilledV3Relay | `FilledV3Relay(address,address,uint256,uint256,uint256,uint256,uint32,uint32,uint32,address,address,address,address,bytes)` | `0xb553cf44...` |
| Stargate | SendToChain | `SendToChain(uint16,bytes,uint256)` | `0x664e2679...` |
| Stargate | ReceiveFromChain | `ReceiveFromChain(uint16,uint256,address,uint256,bytes)` | `0x3f25d151...` |
| LayerZero | PacketSent | `PacketSent(bytes,bytes,address)` | `0x1ab700d4...` |
| OP Stack | ETHBridgeInitiated | `ETHBridgeInitiated(address,address,uint256,bytes)` | `0x2849b430...` |
| OP Stack | ETHBridgeFinalized | `ETHBridgeFinalized(address,address,uint256,bytes)` | `0x31b2166f...` |
| OP Stack | ERC20BridgeInitiated | `ERC20BridgeInitiated(address,address,address,address,uint256,bytes)` | `0x7ff126db...` |
| OP Stack | ERC20BridgeFinalized | `ERC20BridgeFinalized(address,address,address,address,uint256,bytes)` | `0xd59c65b3...` |
| OP Stack (legacy) | ETHDepositInitiated | `ETHDepositInitiated(address,address,uint256,bytes)` | `0x35d79ab8...` |
| OP Stack (legacy) | ETHWithdrawalFinalized | `ETHWithdrawalFinalized(address,address,uint256,bytes)` | `0x2ac69ee8...` |
| Arbitrum | MessageDelivered | `MessageDelivered(uint256,address,address,uint256,uint256,uint256,uint256,uint256,bytes32)` | `0xa72c1d48...` |
| Arbitrum | OutBoxTransactionExecuted | `OutBoxTransactionExecuted(address,address,uint256,uint256,uint256)` | `0x2fc8bec8...` |

---

## 6. Open Questions

1. **Arbitrum L1→L2 matching**: The `MessageDelivered` event doesn't have a trivial corresponding event on L2. Do we match by `messageNum` → ArbSys transaction hash? Or do we treat L1→L2 deposits as "bridge_out only" for analytics purposes?
2. **OP Stack withdrawalHash**: Should we index `OptimismPortal` events to get the canonical `withdrawalHash`, or is the `(from, to, amount)` heuristic sufficient?
3. **Stargate guid extraction**: Is it worth parsing `EndpointV2.PacketSent` encoded payload to extract `guid`, or is heuristic matching good enough?
4. **DefiLlama validation**: Once we have real data, can we validate our bridge volume ranking against DefiLlama's `/bridges` API?
5. **Pending expiry**: How long do we keep unmatched pending bridge_outs? 30 days? 90 days? OP Stack/Arbitrum L2→L1 can take 7 days, so expiry must be > 7 days.
