"""Test full-logs ingestion pipeline: raw logs -> canonical_events."""

from datetime import datetime, timedelta, timezone

import pytest

from core.adapters.evm import EVMAdapter

WETH_BASE = "0x4200000000000000000000000000000000000006"


@pytest.fixture
def adapter():
    a = EVMAdapter(
        chain="base",
        rpc_url="https://mainnet.base.org",
        page_size=200,
    )
    yield a
    a.close()


def test_ingest_raw_fetches_all_logs(adapter: EVMAdapter):
    """Full-logs mode should fetch more logs than filtered mode."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=2)

    raw_logs = list(adapter.ingest_raw(start, end, addresses=WETH_BASE))
    decoded = list(adapter.ingest(start, end, addresses=WETH_BASE))

    assert len(raw_logs) > 0
    assert len(raw_logs) >= len(decoded)

    # All raw logs should have required fields
    for log in raw_logs:
        assert "block_number" in log
        assert "tx_hash" in log
        assert "address" in log
        assert "data" in log
        assert log["source_system"] == "evm_base"


def test_decode_pipeline(adapter: EVMAdapter):
    """Raw logs can be decoded into canonical events."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=2)

    raw_logs = list(adapter.ingest_raw(start, end, addresses=WETH_BASE))
    decoded_events = list(adapter.decode_logs(raw_logs))

    # Should decode some transfer events
    transfers = [e for e in decoded_events if e.event_type == "transfer_out"]
    assert len(transfers) > 0

    for ev in transfers:
        assert ev.source_system == "evm_base"
        assert ev.chain == "base"
        assert ev.token_out == WETH_BASE


def test_unknown_topic0_detection(adapter: EVMAdapter):
    """Detect topic0s not in the current registry."""
    # Synthetic logs with unknown topic0
    raw_logs = [
        {
            "source_system": "evm_base",
            "chain": "base",
            "block_number": 100,
            "tx_hash": "0xdead",
            "log_index": 0,
            "address": "0x1234",
            "topic0": "0x9999999999999999999999999999999999999999999999999999999999999999",
            "data": "0x",
        },
        {
            "source_system": "evm_base",
            "chain": "base",
            "block_number": 100,
            "tx_hash": "0xdead",
            "log_index": 1,
            "address": WETH_BASE,
            "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "data": "0x00000000000000000000000000000000000000000000000000000000000003e8",
        },
    ]

    unknown = adapter.unknown_topic0s(raw_logs)
    assert len(unknown) == 1
    assert "0x999999" in list(unknown.keys())[0]
    assert unknown[list(unknown.keys())[0]] == 1

    # Known topic0 should not appear
    known_topic0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    assert known_topic0 not in unknown
