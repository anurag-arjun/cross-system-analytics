"""End-to-end test for bridge link persistence pipeline."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from core.adapters.base import CanonicalEvent
from core.identity.bridge_links import BridgeLinkEngine
from core.sink import BridgeLinkSink, SinkConfig


class MockClient:
    def __init__(self):
        self.inserts: list[tuple[str, list]] = []

    def insert(self, table: str, data: list) -> None:
        self.inserts.append((table, data))

    def close(self) -> None:
        pass


def _make_bridge_event(event_type, link_key, chain, tx_hash, entity_id, amount=None):
    return {
        "event_type": event_type,
        "event_category": "transaction",
        "link_key": link_key,
        "link_key_type": "across_deposit_id",
        "chain": chain,
        "timestamp": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        "tx_hash": tx_hash,
        "entity_id": entity_id,
        "event_id": f"ev_{tx_hash}",
        "token_out": "0xtoken",
        "amount_out": Decimal(amount) if amount else None,
    }


def test_bridge_link_pipeline():
    """Full pipeline: bridge events -> engine -> sink -> ClickHouse."""
    engine = BridgeLinkEngine()

    outs = [
        _make_bridge_event("bridge_out", "deposit_1", "base", "0xabc1", "0xuser1", "1000"),
        _make_bridge_event("bridge_out", "deposit_2", "base", "0xabc2", "0xuser2", "2000"),
    ]
    ins = [
        _make_bridge_event("bridge_in", "deposit_1", "ethereum", "0xdef1", "0xuser1"),
    ]

    matched = engine.match_batch(outs, ins)
    assert len(matched) == 1
    assert matched[0].link_key == "deposit_1"
    assert matched[0].src_chain == "base"
    assert matched[0].dst_chain == "ethereum"

    mock = MockClient()
    sink = BridgeLinkSink(SinkConfig(batch_size=10), client=mock)
    sink.write(matched)
    sink.close()

    assert sink.total_written == 1
    assert len(mock.inserts) == 1
    assert mock.inserts[0][0] == "bridge_links"
    assert len(mock.inserts[0][1]) == 1

    row = mock.inserts[0][1][0]
    assert row[0] == "deposit_1"
    assert row[1] == "across_deposit_id"
    assert row[2] == "base"
    assert row[6] == "ev_0xabc1"
    assert row[7] == "ethereum"
    assert row[10] == "0xuser1"


def test_bridge_link_with_real_clickhouse():
    """Integration test with local ClickHouse (requires docker-compose up)."""
    pytest.importorskip("clickhouse_connect")

    from clickhouse_connect import get_client

    try:
        client = get_client(host="localhost", port=8124, username="default", password="nexus")
        client.command("SELECT 1")
    except Exception:
        pytest.skip("ClickHouse not available")

    engine = BridgeLinkEngine()
    outs = [
        _make_bridge_event("bridge_out", "deposit_test", "base", "0xabc", "0xuser", "5000"),
    ]
    ins = [
        _make_bridge_event("bridge_in", "deposit_test", "ethereum", "0xdef", "0xuser"),
    ]

    matched = engine.match_batch(outs, ins)

    sink = BridgeLinkSink(SinkConfig(host="localhost", port=8124, password="nexus", batch_size=10))
    sink.write(matched)
    sink.close()

    result = client.query(
        "SELECT link_key, src_chain, dst_chain, amount FROM bridge_links WHERE link_key = 'deposit_test'"
    )
    assert len(result.result_rows) == 1
    row = result.result_rows[0]
    assert row[0] == "deposit_test"
    assert row[1] == "base"
    assert row[2] == "ethereum"
    assert row[3] == 5000

    client.command("DELETE FROM bridge_links WHERE link_key = 'deposit_test'")
    client.close()
