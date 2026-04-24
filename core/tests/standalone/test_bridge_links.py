"""Tests for bridge link matching engine."""

from datetime import datetime, timezone

import pytest

from core.identity.bridge_links import BridgeLinkEngine


class TestBridgeLinkEngine:
    def test_single_match(self):
        engine = BridgeLinkEngine()

        bridge_out = {
            "event_type": "bridge_out",
            "link_key": "deposit_123",
            "link_key_type": "across_deposit_id",
            "chain": "base",
            "timestamp": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            "tx_hash": "0xabc",
            "entity_id": "0xuser1",
            "event_id": "ev1",
            "token_out": "0xtoken",
            "amount_out": 1000,
        }

        bridge_in = {
            "event_type": "bridge_in",
            "link_key": "deposit_123",
            "link_key_type": "across_deposit_id",
            "chain": "ethereum",
            "timestamp": datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
            "tx_hash": "0xdef",
            "entity_id": "0xuser1",
            "event_id": "ev2",
        }

        engine.add_bridge_out(bridge_out)
        link = engine.add_bridge_in(bridge_in)

        assert link is not None
        assert link.link_key == "deposit_123"
        assert link.src_chain == "base"
        assert link.dst_chain == "ethereum"
        assert link.src_tx_hash == "0xabc"
        assert link.dst_tx_hash == "0xdef"
        assert link.amount == 1000

    def test_no_match_different_key(self):
        engine = BridgeLinkEngine()

        bridge_out = {
            "event_type": "bridge_out",
            "link_key": "deposit_123",
            "link_key_type": "across_deposit_id",
            "chain": "base",
            "timestamp": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            "tx_hash": "0xabc",
            "entity_id": "0xuser1",
            "event_id": "ev1",
        }

        bridge_in = {
            "event_type": "bridge_in",
            "link_key": "deposit_456",
            "link_key_type": "across_deposit_id",
            "chain": "ethereum",
            "timestamp": datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
            "tx_hash": "0xdef",
            "entity_id": "0xuser1",
            "event_id": "ev2",
        }

        engine.add_bridge_out(bridge_out)
        link = engine.add_bridge_in(bridge_in)

        assert link is None
        assert len(engine.get_pending()) == 1

    def test_batch_match(self):
        engine = BridgeLinkEngine()

        outs = [
            {
                "event_type": "bridge_out",
                "link_key": f"deposit_{i}",
                "link_key_type": "across_deposit_id",
                "chain": "base",
                "timestamp": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                "tx_hash": f"0xabc{i}",
                "entity_id": "0xuser1",
                "event_id": f"ev_out_{i}",
                "token_out": "0xtoken",
                "amount_out": 1000 * i,
            }
            for i in range(3)
        ]

        ins = [
            {
                "event_type": "bridge_in",
                "link_key": f"deposit_{i}",
                "link_key_type": "across_deposit_id",
                "chain": "ethereum",
                "timestamp": datetime(2024, 1, 1, 12, 5, 0, tzinfo=timezone.utc),
                "tx_hash": f"0xdef{i}",
                "entity_id": "0xuser1",
                "event_id": f"ev_in_{i}",
            }
            for i in range(2)
        ]

        matched = engine.match_batch(outs, ins)

        assert len(matched) == 2
        assert len(engine.get_pending()) == 1
        assert engine.get_pending()[0].link_key == "deposit_2"

    def test_stats(self):
        engine = BridgeLinkEngine()

        engine.add_bridge_out(
            {
                "event_type": "bridge_out",
                "link_key": "key1",
                "chain": "base",
                "timestamp": datetime.now(timezone.utc),
                "tx_hash": "0xabc",
                "entity_id": "0xuser",
                "event_id": "ev1",
            }
        )

        engine.add_bridge_in(
            {
                "event_type": "bridge_in",
                "link_key": "key1",
                "chain": "ethereum",
                "timestamp": datetime.now(timezone.utc),
                "tx_hash": "0xdef",
                "entity_id": "0xuser",
                "event_id": "ev2",
            }
        )

        stats = engine.stats()
        assert stats["completed"] == 1
        assert stats["pending"] == 0

    def test_missing_link_key_skipped(self):
        engine = BridgeLinkEngine()

        bridge_out = {
            "event_type": "bridge_out",
            "link_key": None,
            "chain": "base",
            "timestamp": datetime.now(timezone.utc),
            "tx_hash": "0xabc",
            "entity_id": "0xuser",
            "event_id": "ev1",
        }

        engine.add_bridge_out(bridge_out)
        assert len(engine.get_pending()) == 0
