# Bridge Research — Verified from Spellbook + Source Contracts

**Date**: 2026-05-06  
**Sources**: duneanalytics/spellbook (cloned), contract repos, on-chain verification

---

## Critical Discovery: LayerZero V1 vs V2 Endpoint IDs

**Spellbook uses LayerZero V1 endpoint IDs** (from function call traces), which are DIFFERENT from V2 EIDs:

| Chain | LZ V1 ID (Spellbook) | LZ V2 EID | EVM Chain ID |
|---|---|---|---|
| Ethereum | 101 | 30101 | 1 |
| Arbitrum | 110 | 30110 | 42161 |
| Optimism | 111 | 30111 | 10 |
| Base | 184 | 30184 | 8453 |
| Polygon | 109 | 30109 | 137 |

**Stargate V2 uses V2 EIDs** in `ReceiveFromChain.srcEid` (30184 for Base), but `SendToChain.dstChainId` uses EVM chain IDs (8453 for Base). These will never match directly.

---

## Bridge-by-Bridge Analysis (Spellbook-verified)

### 1. Across V3 ✅

**Spellbook approach**: Uses Dune-decoded event table `base_spokepool_evt_v3fundsdeposited`  
**Chain mapping**: Standard EVM chain IDs (1, 10, 8453, 42161...)  
**Bridge transfer ID**: `depositId` (uint32 from event)  
**Our status**: ✅ Matching works. `deposit_id` is correct key.  
**Chain index source**: `bridges_across_chain_indexes` — EVM chain_id → chain name

### 2. LayerZero V1 (Spellbook) / V2 (Stargate)

**Spellbook approach**: Uses **function call traces** (`Endpoint_call_send`), not events!  
- Decodes `_dstChainId` from the `send()` function parameters  
- Joins with token transfers to get amounts  
- Maps chains via `bridges_layerzero_chain_indexes` (V1 IDs)  

**Our approach**: Uses event logs (`SendToChain`, `ReceiveFromChain`)  
- `SendToChain(uint16 dstChainId, bytes to, uint256 qty)` — uses EVM chain IDs  
- `ReceiveFromChain(uint16 srcEid, uint256 sender, address receiver, uint256 amount, bytes message)` — uses LZ V2 EIDs  
- **MISMATCH**: `dstChainId` (8453) ≠ `srcEid` (30184) for Base  

**Fix for na-7qle**: Two-step mapping  
```
dst_chain_id (8453) → chain_name (base) ← src_eid (30184) → match!
```

### 3. CCTP (Circle Cross-Chain Transfer Protocol)

**Spellbook approach**: `tokenmessenger_evt_depositforburn` event  
- `destinationDomain` as chain ID (CCTP-specific domain IDs: 0=ethereum, 6=base...)  
- `nonce` as bridge_transfer_id  
**Chain mapping**: `bridges_cctp_chain_indexes` (CCTP domain → chain name)  
**Our status**: Not implemented

### 4. Arbitrum Native Bridge

**Spellbook approach**: Uses **gateway events**, NOT Inbox/Outbox  
- Deposit (L1→L2): `l1erc20gateway_evt_depositinitiated`  
  - bridge_transfer_id = `_sequenceNumber`  
- Withdrawal (L2→L1): `l1erc20gateway_evt_withdrawalfinalized`  
  - bridge_transfer_id = `_exitNum`  
**Chain mapping**: Hardcoded (42161 for Arbitrum)  
**Our status**: Need to use gateway events, not Inbox/Outbox

### 5. OP Stack Native Bridge

**Spellbook approach**: Static contract address labels only (no event decoding)  
**Our approach**: ETHBridgeInitiated/Finalized + ERC20BridgeInitiated/Finalized event decoders  
**Our status**: ✅ Decoders done (na-ma17). Matching is tx_hash-based heuristic, not yet precise.

---

## Verified Topic0 Hashes (on-chain confirmed)

| Event | Topic0 | Ethereum | Base | Arbitrum |
|---|---|---|---|---|
| Stargate SendToChain | `0x664e2679...` | — | ✅ (1) | — |
| Stargate ReceiveFromChain | `0x3f25d151...` | 0 | 0 | 0 |
| Across V3FundsDeposited | `0xa123dc29...` | 0 | 0 | 0 |
| Across FilledV3Relay | `0xb553cf44...` | 0 | 0 | 0 |
| OP ETHBridgeInitiated | `0x2849b430...` | ✅ | ✅ | — |
| OP ERC20BridgeInitiated | `0x7ff126db...` | ✅ | ✅ | — |
| OP ETHBridgeFinalized | `0x31b2166f...` | ✅ (52) | ✅ (26) | — |
| OP ERC20BridgeFinalized | `0xd59c65b3...` | ✅ (68) | ✅ (12) | — |

**Note on Across/Stargate**: 0 events in recent ~4hr windows on our chains. These protocols may have lower activity on Base/Ethereum vs L2-focused chains, or our topic0 hashes may be wrong. Need to verify against a longer time window.

---

## What We Need to Fix (Priority Order)

### na-7qle: Fix Stargate Bridge Matching
1. Add `chain_id_to_name` mapping: `{8453: "base", 42161: "arbitrum", 10: "optimism", 1: "ethereum"}`
2. Add `eid_to_name` mapping: `{30184: "base", 30110: "arbitrum", 30111: "optimism", 30101: "ethereum"}`
3. In BridgeLinkEngine: normalize both link_keys to chain names, match when names equal
4. Heuristic: entity match + amount within 0.5% + time < 30min

### na-250m: LayerZero GUID-based Matching (covers ALL LZ bridges)
Per Spellbook approach: use function call traces if available, or reconstruct GUID from events

### na-xk8z: Arbitrum Native Bridge Decoders
Per Spellbook approach: use gateway events (DepositInitiated, WithdrawalFinalized), not Inbox/Outbox

### na-dml4: OP Stack Precise Matching
Per our ADR-002: use OptimismPortal WithdrawalProven/WithdrawalFinalized for withdrawalHash matching
