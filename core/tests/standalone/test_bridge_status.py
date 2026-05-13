"""Unit tests for the bridge transaction status classifier.

These tests pin the contract between the SQL row producer and the
classifier. Each test names the input shape and asserts the resulting
(status, tags, reason).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core.identity.bridge_status import (
    BridgeConfig,
    Status,
    Tag,
    classify,
)

NOW = datetime(2026, 5, 13, 18, 0, 0, tzinfo=timezone.utc)


def _pair(**kw) -> dict:
    base = {
        "row_type": "pair",
        "bridge": "across",
        "link_key": "42161:5000",
        "link_key_type": "across_deposit_id",
        "src_chain": "arbitrum",
        "dst_chain": "base",
        "src_block_time": NOW - timedelta(seconds=30),
        "dst_block_time": NOW - timedelta(seconds=22),
        "src_entity_id": "0xabc",
        "dst_entity_id": "0xabc",
        "src_token": "0xUSDC",
        "dst_token": "0xUSDC",
        "src_amount": "1000.0",
        "dst_amount": "999.5",
        "src_amount_usd": 1000.0,
        "dst_amount_usd": 999.5,
        "latency_seconds": 8,
    }
    base.update(kw)
    return base


def _orphan_out(**kw) -> dict:
    base = {
        "row_type": "orphan_out",
        "bridge": "across",
        "link_key_type": "across_deposit_id",
        "src_chain": "arbitrum",
        "src_block_time": NOW - timedelta(hours=2),
        "src_entity_id": "0xabc",
        "src_token": "0xUSDC",
        "src_amount": "1000.0",
        "src_amount_usd": 1000.0,
        "dst_chain_id_hint": None,
    }
    base.update(kw)
    return base


def _orphan_in(**kw) -> dict:
    base = {
        "row_type": "orphan_in",
        "bridge": "layerzero",
        "link_key_type": "layerzero_src_eid",
        "dst_chain": "base",
        "dst_block_time": NOW - timedelta(minutes=5),
        "dst_entity_id": "0xdef",
        "dst_token": "0xUSDC",
        "dst_amount": "999.5",
        "src_chain_id_hint": None,
    }
    base.update(kw)
    return base


# ---------------------------------------------------------------------------
# Happy-path matched rows
# ---------------------------------------------------------------------------


def test_pair_clean_match():
    out = classify(_pair(), now=NOW)
    assert out["status"] == Status.MATCHED.value
    assert out["tags"] == []


def test_pair_negative_latency_tagged():
    out = classify(_pair(latency_seconds=-5), now=NOW)
    assert Tag.NEGATIVE_LATENCY.value in out["tags"]


def test_pair_latency_outlier_for_fast_bridge():
    # Across typical_latency=180s; outlier threshold is 10× that or finality.
    out = classify(_pair(latency_seconds=10000), now=NOW)
    assert Tag.LATENCY_OUTLIER.value in out["tags"]


def test_pair_same_chain_tagged():
    out = classify(_pair(src_chain="arbitrum", dst_chain="arbitrum"), now=NOW)
    assert Tag.SAME_CHAIN.value in out["tags"]


def test_pair_recipient_differs_tagged():
    out = classify(_pair(src_entity_id="0xa", dst_entity_id="0xb"), now=NOW)
    assert Tag.RECIPIENT_DIFFERS.value in out["tags"]


def test_pair_amount_mismatch_tagged():
    # Across tolerance = 2%; 20% gap should fire.
    out = classify(_pair(src_amount="1000", dst_amount="800"), now=NOW)
    assert Tag.AMOUNT_MISMATCH.value in out["tags"]


def test_pair_amount_mismatch_NOT_tagged_within_tolerance():
    # 1% gap is within Across's 2% tolerance.
    out = classify(_pair(src_amount="1000", dst_amount="990"), now=NOW)
    assert Tag.AMOUNT_MISMATCH.value not in out["tags"]


def test_pair_no_usd_value_tagged():
    out = classify(_pair(src_amount_usd=None, dst_amount_usd=None), now=NOW)
    assert Tag.NO_USD_VALUE.value in out["tags"]


def test_pair_token_changed_tagged_for_same_chain_swap():
    # Cross-chain pairs always have different token addresses (USDC on eth
    # ≠ USDC on base) so TOKEN_CHANGED only fires for same-chain bridges.
    out = classify(
        _pair(src_chain="arbitrum", dst_chain="arbitrum",
              src_token="0xUSDC", dst_token="0xWETH"),
        now=NOW,
    )
    assert Tag.TOKEN_CHANGED.value in out["tags"]


def test_pair_token_changed_NOT_tagged_cross_chain():
    # Cross-chain Across with different addresses — expected, not a signal.
    out = classify(_pair(src_token="0xUSDC_op", dst_token="0xUSDC_eth"), now=NOW)
    assert Tag.TOKEN_CHANGED.value not in out["tags"]


def test_pair_token_changed_NOT_tagged_for_layerzero_oft():
    out = classify(
        _pair(bridge="layerzero",
              src_chain="ethereum", dst_chain="ethereum",
              src_token="0xUSDC", dst_token="0xUSDT"),
        now=NOW,
    )
    assert Tag.TOKEN_CHANGED.value not in out["tags"]


def test_pair_multi_match_flag_propagates():
    out = classify(_pair(multi_match_src=True), now=NOW)
    assert Tag.MULTI_MATCH.value in out["tags"]


# ---------------------------------------------------------------------------
# Orphan_out — bridge_out with no matching bridge_in
# ---------------------------------------------------------------------------


def test_orphan_out_dst_out_of_scope_when_chain_unmonitored():
    # dst_chain_id 56 = BNB (not in our 5 chains)
    out = classify(_orphan_out(dst_chain_id_hint="56"), now=NOW)
    assert out["status"] == Status.UNMATCHED_DST_OUT_OF_SCOPE.value
    assert "bnb" in out["reason"].lower() or "56" in out["reason"]


def test_orphan_out_dst_out_of_scope_unknown_chain_id():
    # Unknown chain id (e.g., HyperLiquid 999) → still out of scope
    out = classify(_orphan_out(dst_chain_id_hint="999"), now=NOW)
    assert out["status"] == Status.UNMATCHED_DST_OUT_OF_SCOPE.value


def test_orphan_out_decoder_gap_when_bridge_in_missing():
    # CCTP has no bridge_in decoder
    out = classify(
        _orphan_out(bridge="cctp", link_key_type="cctp_nonce", dst_chain_id_hint=None),
        now=NOW,
    )
    assert out["status"] == Status.UNMATCHED_DECODER_GAP.value
    assert "bridge_in" in out["reason"]


def test_orphan_out_broken_matcher_for_op_stack():
    out = classify(
        _orphan_out(bridge="op_stack", link_key_type="op_stack_bridge", dst_chain_id_hint=None),
        now=NOW,
    )
    assert out["status"] == Status.UNMATCHED_BROKEN_MATCHER.value


def test_orphan_out_in_flight_for_recent_across():
    # Across typical_latency = 180s; row is 60s old → in-flight (< 3× window).
    out = classify(
        _orphan_out(src_block_time=NOW - timedelta(seconds=60), dst_chain_id_hint="8453"),
        now=NOW,
    )
    assert out["status"] == Status.IN_FLIGHT.value


def test_orphan_out_unknown_for_old_across():
    # Across, 2h old, dst is in scope → past the typical latency window.
    out = classify(
        _orphan_out(src_block_time=NOW - timedelta(hours=2), dst_chain_id_hint="8453"),
        now=NOW,
    )
    assert out["status"] == Status.UNMATCHED_UNKNOWN.value


def test_orphan_out_pending_finality_for_op_stack_withdrawal():
    # Override matcher_broken=False to test the finality path directly.
    cfg = {
        "op_stack": BridgeConfig(
            typical_latency_seconds=600, finality_seconds=7*86400,
            bridge_out_decoded=True, bridge_in_decoded=True,
            matcher_broken=False,
        ),
    }
    out = classify(
        _orphan_out(bridge="op_stack", link_key_type="op_stack_bridge",
                    src_block_time=NOW - timedelta(days=3),
                    dst_chain_id_hint=None),
        now=NOW, bridges=cfg,
    )
    assert out["status"] == Status.PENDING_FINALITY.value


# ---------------------------------------------------------------------------
# Orphan_in — bridge_in with no matching bridge_out
# ---------------------------------------------------------------------------


def test_orphan_in_src_out_of_scope():
    # LayerZero PacketDelivered with src_eid=30102 (BNB) → out of scope
    out = classify(_orphan_in(src_chain_id_hint="30102"), now=NOW)
    assert out["status"] == Status.UNMATCHED_SRC_OUT_OF_SCOPE.value
    assert "bnb" in out["reason"].lower() or "30102" in out["reason"]


def test_orphan_in_decoder_gap_when_bridge_out_missing():
    # arbitrum_bridge has bridge_out_decoded=False (L1 deposit decoder TODO)
    out = classify(
        _orphan_in(bridge="arbitrum_bridge", link_key_type="arbitrum_outbox_index",
                   src_chain_id_hint=None),
        now=NOW,
    )
    assert out["status"] == Status.UNMATCHED_DECODER_GAP.value


def test_orphan_in_broken_matcher_for_op_stack():
    out = classify(
        _orphan_in(bridge="op_stack", link_key_type="op_stack_bridge",
                   src_chain_id_hint=None),
        now=NOW,
    )
    assert out["status"] == Status.UNMATCHED_BROKEN_MATCHER.value


def test_orphan_in_unknown_when_in_scope_with_decoder():
    # LayerZero, src eid 30184=base (in scope), both decoders exist.
    # No matching out → unknown bug or data gap.
    out = classify(_orphan_in(src_chain_id_hint="30184"), now=NOW)
    assert out["status"] == Status.UNMATCHED_UNKNOWN.value


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_unknown_row_type_returns_unknown():
    out = classify({"row_type": "garbage", "bridge": "across"}, now=NOW)
    assert out["status"] == Status.UNMATCHED_UNKNOWN.value


def test_unknown_bridge_uses_default_config():
    # Bridge name we don't have a profile for → default config applied.
    out = classify(
        _orphan_out(bridge="unknown_bridge", link_key_type="x", dst_chain_id_hint=None,
                    src_block_time=NOW - timedelta(days=1)),
        now=NOW,
    )
    # Default has both decoders=True, no broken matcher → fall through to UNKNOWN.
    assert out["status"] == Status.UNMATCHED_UNKNOWN.value
