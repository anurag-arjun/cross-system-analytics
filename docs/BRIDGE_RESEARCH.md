# Bridge Family Research — Complete Landscape

**Date**: 2026-05-06  
**Scope**: All bridge families, event signatures, matching strategies, on-chain verification

> **Topic0 sources**: All event signatures derived from source contract repositories  
> (ethereum-optimism/optimism, LayerZero-Labs/LayerZero-v2, OffchainLabs/nitro-contracts).  
> Spellbook (duneanalytics/spellbook) uses a different approach — per-chain pool Swap  
> events and static address labels, no cross-chain link_key matching.

---

## 1. Intent Bridges

### 1.1 Across V3 ✅
| | Source Chain | Dest Chain |
|---|---|---|
| Event | `V3FundsDeposited` | `FilledV3Relay` |
| Type | bridge_out | bridge_in |
| Match key | `deposit_id` (uint32) | `deposit_id` (uint32, indexed topic[3]) |
| Status | Both decoders implemented, Postgres matching verified | |

### 1.2 deBridge DLN (not ticketed)
| | Source Chain | Dest Chain |
|---|---|---|
| Event | `CreatedOrder` | `FulfilledOrder` |
| Match key | `orderId` (bytes32) | `orderId` |

---

## 2. Message-Passing Bridges (LayerZero)

### 2.1 LayerZero V2 Endpoint (general solution — na-250m)

**Contracts**: `EndpointV2` on each chain  
**Core events verified on-chain**:

| Event | Ethereum | Base | Arbitrum | Topic0 |
|---|---|---|---|---|
| `PacketSent(bytes,bytes,address)` | 3,272 | 863 | 120 | `0x1ab700d4...` |
| `PacketDelivered((uint32,bytes32,uint64),address)` | 3,580 | — | 95 | `0x3cd5e48f...` |

**GUID (global unique identifier)**:  
```
GUID = keccak256(nonce, srcEid, sender, dstEid, receiver)
```
- Generated on source chain during `Endpoint.send()`
- Passed to `Endpoint.lzReceive()` on destination chain as `_guid` parameter
- Emitted as `MessagingReceipt.guid` in the `send()` return (off-chain queryable)
- **NOT** directly in `PacketSent` event — needs to be reconstructed from the encodedPayload

**Matching approach**: Decode `PacketSent.encodedPayload` to extract GUID, match `PacketDelivered` by reconstructing GUID from Origin struct fields (nonce + srcEid + sender + dstEid + receiver).

### 2.2 Stargate V2 (na-7qle)

**Current decoders**:
```
SendToChain(uint16 dstChainId, bytes to, uint256 qty)
  → bridge_out, link_key=dst_chain_id (e.g., 8453 for Base)

ReceiveFromChain(uint16 srcEid, uint256 sender, address receiver, uint256 amount, bytes message)  
  → bridge_in, link_key=src_eid (e.g., 30184 for Base)
```

**The mismatch**: `dstChainId` (8453) ≠ `srcEid` (30184) for the same chain.  
**Bridge matching always fails** because link_keys never match.

### 2.3 Endpoint ID (EID) Mapping

| Chain | Chain ID | Endpoint ID (EID) |
|---|---|---|
| Ethereum | 1 | 30101 |
| Arbitrum | 42161 | 30110 |
| Optimism | 10 | 30111 |
| Base | 8453 | 30184 |
| Avalanche | 43114 | 30106 |
| Polygon | 137 | 30109 |

**Fix options for na-7qle**:

**Option A — EID mapping** (simpler, targeted):  
1. Add `eid_to_chain` and `chain_id_to_chain` mapping dicts  
2. In `BridgeLinkEngine`, resolve both sides to chain names  
3. Match when `chain_from(dst_chain_id) ↔ chain_from(src_eid)`  
4. Add heuristic: to ≈ receiver, amount within 0.5%, time < 30min  

**Option B — LayerZero GUID** (proper, general):  
1. Build `LayerZeroGuidExtractor`: decode `PacketSent.encodedPayload` to get GUID  
2. Build `PacketDeliveredDecoder`: extract Origin (srcEid, sender, nonce)  
3. Match bridge_out ↔ bridge_in via the LayerZero GUID  
4. Covers Stargate + all LayerZero-based bridges (OFT, etc.)

**Recommendation**: Start with Option A for quick Stargate fix, then Option B via na-250m for general LayerZero support.

---

## 3. Canonical Bridges

### 3.1 OP Stack ✅ (bridge_initiated + finalized done, precise matching via na-dml4)

**L1→L2 deposit** (done):
| Chain | Event | Type | Decoder |
|---|---|---|---|
| Ethereum | `ETHBridgeInitiated` / `ERC20BridgeInitiated` | bridge_out | ✅ `BaseETHBridgeInitiatedDecoder` |
| Base | `ETHBridgeFinalized` / `ERC20BridgeFinalized` | bridge_in | ✅ `ETHBridgeFinalizedDecoder` |

**L2→L1 withdrawal** (needs precise matching):
| Chain | Event | Type | Topic0 | Live |
|---|---|---|---|---|
| L2 (Base) | `WithdrawalInitiated` | bridge_out | (via `L2ToL1MessagePasser`) | — |
| L1 (Eth) | `WithdrawalProven` | — | `0x67a6208c...` | 230 ✅ |
| L1 (Eth) | `WithdrawalFinalized` | — | `0xdb5c7652...` | 268 ✅ |

**Precise matching (ADR-002)**: Extract `withdrawalHash` from `WithdrawalProven`/`WithdrawalFinalized` (indexed topic[1]). Match with the same hash from the L2 `MessagePassed` event. The `withdrawalHash` is: `keccak256(abi.encode(version, nonce, sender, target, value, gasLimit, data))`.

### 3.2 Arbitrum Nitro (na-xk8z)

| Chain | Event | Type | Topic0 | Live |
|---|---|---|---|---|
| Ethereum | `InboxMessageDelivered(uint256,bytes)` | bridge_out (L1→L2) | `0xff64905f...` | 1,418 ✅ |
| Arbitrum | (no explicit finalize event) | bridge_in | — | — |
| Ethereum | `OutBoxTransactionExecuted(address,address,uint256,uint256)` | bridge_in (L2→L1) | `0x20af7f3b...` | 57 ✅ |

**`InboxMessageDelivered(uint256 indexed messageNum, bytes data)`**: messageNum is the L1→L2 message sequence number (uplink). Used to order retryable tickets.

**`OutBoxTransactionExecuted(address indexed destAddr, address indexed l2Sender, uint256 indexed outboxEntryIndex, uint256 transactionIndex)`**: outboxEntryIndex is the unique identifier for the L2→L1 message. Emitted when the outbox executes the withdrawal.

**Matching approach**:
- L1→L2: `messageNum` from Inbox on L1 (bridge_out). On L2, the `ArbSys` precompile at `0x64` receives the message via `L2ToL1MessagePasser`. No explicit finalize event on L2 — the retryable ticket auto-redeems. **Document limitation: L1→L2 bridge_in can only be detected by successful redemption of the retryable.**
- L2→L1: On L2, `ArbSys.sendTxToL1` initiates → after ~7 day challenge → `OutBoxTransactionExecuted` on L1 with `outboxEntryIndex` as the match key.

---

## 4. Matching Strategy Comparison

| Bridge Family | Approach | Match Key | Latency | Retry Cadence |
|---|---|---|---|---|
| Across V3 | deposit_id | uint32 | 2-5 min | 2 min |
| Stargate V2 | EID mapping + heuristic (→ GUID) | uint16 → bytes32 | 2-10 min | 10 min |
| OP Stack L1→L2 | tx_hash heuristic (current), withdrawalHash (future) | bytes32 | 2-5 min | 6 hr |
| OP Stack L2→L1 | **withdrawalHash** (ADR-002) | bytes32 | 7 days | 6 hr |
| Arbitrum L1→L2 | messageNum | uint256 | 10 min | 6 hr |
| Arbitrum L2→L1 | outboxEntryIndex (ADR-002) | uint256 | 7 days | 6 hr |

---

## 5. Next Actions (priority order)

1. **na-7qle** — Fix Stargate with EID mapping (Option A)  
2. **na-250m** — Add LayerZero GUID-based matching (Option B, covers all LZ bridges)  
3. **na-dml4** — Add OptimismPortal WithdrawalProven/Finalized decoders for precise OP Stack L2→L1 matching  
4. **na-xk8z** — Add Arbitrum InboxMessageDelivered + OutBoxTransactionExecuted decoders  

---

## 6. Decoder Implementation Status

| Decoder | Bridge | Chain(s) | Topic0 | Status |
|---|---|---|---|---|
| AcrossV3FundsDeposited | Across | * | `0xa123dc29...` | ✅ |
| AcrossV3FilledRelay | Across | * | `0xb553cf44...` | ✅ |
| StargateSendToChain | Stargate | * | `0x664e2679...` | ✅ |
| StargateReceiveFromChain | Stargate | * | `0x3f25d151...` | ✅ |
| BaseETHBridgeInitiated | OP Stack | * | `0x2849b430...` | ✅ |
| BaseERC20BridgeInitiated | OP Stack | * | `0x7ff126db...` | ✅ |
| ETHBridgeFinalized | OP Stack | * | `0x31b2166f...` | ✅ |
| ERC20BridgeFinalized | OP Stack | * | `0xd59c65b3...` | ✅ |
| LayerZeroPacketSent | LZ V2 | * | `0x1ab700d4...` | 🔲 |
| LayerZeroPacketDelivered | LZ V2 | * | `0x3cd5e48f...` | 🔲 |
| OPWithdrawalProven | OP Stack | ethereum | `0x67a6208c...` | 🔲 |
| OPWithdrawalFinalized | OP Stack | ethereum | `0xdb5c7652...` | 🔲 |
| ArbInboxMessageDelivered | Arb Nitro | ethereum | `0xff64905f...` | 🔲 |
| ArbOutBoxTransactionExecuted | Arb Nitro | ethereum | `0x20af7f3b...` | 🔲 |
