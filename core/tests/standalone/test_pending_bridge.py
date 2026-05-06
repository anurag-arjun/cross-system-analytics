"""Tests for PendingBridgeStore implementations."""

from datetime import datetime, timedelta, timezone

import pytest

from core.identity.pending_bridge import (
    DEFAULT_RETRY_CADENCE,
    InMemoryPendingBridgeStore,
    PendingBridgeOut,
    RETRY_CADENCES,
)


def _make_bridge_out_event(
    event_id="ev1",
    link_key="dep_1",
    link_key_type="across_deposit_id",
    chain="base",
    timestamp=None,
):
    if timestamp is None:
        timestamp = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    return {
        "event_id": event_id,
        "entity_id": "0xaaa",
        "entity_type": "wallet",
        "event_type": "bridge_out",
        "link_key": link_key,
        "link_key_type": link_key_type,
        "chain": chain,
        "timestamp": timestamp,
        "tx_hash": "0xtx1",
        "token_out": "ETH",
        "amount_out": "1500000000000000000",
    }


class TestInMemoryPendingBridgeStore:
    def test_add_pending(self):
        store = InMemoryPendingBridgeStore()
        ev = _make_bridge_out_event()
        assert store.add_pending(ev) is True
        assert store.add_pending(ev) is False  # duplicate event_id

    def test_get_pending_by_link_key(self):
        store = InMemoryPendingBridgeStore()
        store.add_pending(_make_bridge_out_event("ev1", "dep_1", "across_deposit_id", "base"))
        store.add_pending(_make_bridge_out_event("ev2", "dep_1", "across_deposit_id", "ethereum"))
        store.add_pending(_make_bridge_out_event("ev3", "dep_2", "across_deposit_id", "base"))

        results = store.get_pending_by_link_key("dep_1", "across_deposit_id")
        assert len(results) == 2

    def test_mark_matched(self):
        store = InMemoryPendingBridgeStore()
        store.add_pending(_make_bridge_out_event("ev1", "dep_1"))
        assert store.mark_matched("ev1", "ethereum", datetime.now(timezone.utc), "0xtx2", "0xbbb", "ev2")
        results = store.get_pending_by_link_key("dep_1", "across_deposit_id")
        assert len(results) == 0

    def test_delete_expired(self):
        store = InMemoryPendingBridgeStore()
        # Create an event that expired 1 day ago.
        ev = _make_bridge_out_event("ev1", "dep_1")
        row = PendingBridgeOut.from_canonical_event(ev)
        row.expires_at = datetime.now(timezone.utc) - timedelta(days=1)
        store._rows["ev1"] = row
        deleted = store.delete_expired(datetime.now(timezone.utc))
        assert deleted == 1

    def test_get_pending_for_retry(self):
        store = InMemoryPendingBridgeStore()
        store.add_pending(_make_bridge_out_event("ev1", "dep_1"))
        results = store.get_pending_for_retry(datetime.now(timezone.utc) + timedelta(days=1))
        assert len(results) == 1

    def test_update_retry_schedule(self):
        store = InMemoryPendingBridgeStore()
        store.add_pending(_make_bridge_out_event("ev1", "dep_1"))
        assert store.update_retry_schedule("ev1") is True
        row = store.get_pending_for_retry(datetime.max.replace(tzinfo=timezone.utc))[0]
        assert row.retry_count == 1

    def test_cross_chain_deposit_id_distinct(self):
        """depositId=1 on Base and Ethereum must not collide."""
        store = InMemoryPendingBridgeStore()
        store.add_pending(_make_bridge_out_event("ev_base", "1", "across_deposit_id", "base"))
        store.add_pending(_make_bridge_out_event("ev_eth", "1", "across_deposit_id", "ethereum"))

        results = store.get_pending_by_link_key("1", "across_deposit_id")
        assert len(results) == 2
        chains = {r.src_chain for r in results}
        assert chains == {"base", "ethereum"}


class TestPendingBridgeOut:
    def test_from_canonical_event_sets_retry(self):
        ev = _make_bridge_out_event(link_key_type="across_deposit_id")
        row = PendingBridgeOut.from_canonical_event(ev)
        assert row.status == "pending"
        assert row.retry_count == 0
        expected = datetime.now(timezone.utc) + RETRY_CADENCES["across_deposit_id"]
        assert abs((row.next_retry_at - expected).total_seconds()) < 5

    def test_default_retry_for_unknown_type(self):
        ev = _make_bridge_out_event(link_key_type="unknown_bridge")
        row = PendingBridgeOut.from_canonical_event(ev)
        expected = datetime.now(timezone.utc) + DEFAULT_RETRY_CADENCE
        assert abs((row.next_retry_at - expected).total_seconds()) < 5
