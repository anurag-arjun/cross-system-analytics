"""Standalone test for EVM adapter — fetches a small block range from Base.

Requirements: Internet access (Pocket Network public RPC)
No local infra required.
"""

from datetime import datetime, timedelta, timezone

import pytest

from core.adapters.evm import EVMAdapter


@pytest.fixture
def adapter():
    a = EVMAdapter(
        chain="base",
        rpc_url="https://mainnet.base.org",
        page_size=200,
    )
    yield a
    a.close()


def test_rpc_connection(adapter: EVMAdapter):
    head = adapter._ensure_rpc().get_block_number()
    assert head > 20_000_000


WETH_BASE = "0x4200000000000000000000000000000000000006"


def test_fetch_transfer_logs(adapter: EVMAdapter):
    """Fetch Transfer events from the last ~2 minutes on Base."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=2)

    events = list(adapter.ingest(start, end, addresses=WETH_BASE))

    # We should get *some* transfers on Base in 30 min (high activity)
    assert len(events) > 0, "No transfer events found in the last 30 min"

    ev = events[0]
    assert ev.event_type == "transfer_out"
    assert ev.entity_type == "wallet"
    assert ev.source_system == "evm_base"
    assert ev.chain == "base"
    assert ev.block_number is not None
    assert ev.tx_hash is not None
    assert ev.log_index is not None
    assert ev.token_out is not None
    assert ev.amount_out is not None
    assert ev.amount_out > 0
    assert ev.counterparty is not None

    # Validate event_id is deterministic sha256
    assert len(ev.event_id) == 64


def test_decode_known_transfer(adapter: EVMAdapter):
    """Decode a synthetic Transfer log without hitting the network."""
    log = {
        "address": "0x4200000000000000000000000000000000000006",
        "blockHash": "0x1234",
        "blockNumber": "0x2afec00",
        "data": "0x00000000000000000000000000000000000000000000000000000000000003e8",
        "logIndex": "0x0",
        "removed": False,
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
            "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2",
        ],
        "transactionHash": "0xdeadbeef",
        "transactionIndex": "0x0",
    }

    # Mock block time lookup
    adapter._block_ts[0x2AFEC00] = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    ev = adapter._decode_log(log)
    assert ev.event_type == "transfer_out"
    assert ev.entity_id == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    assert ev.counterparty == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2"
    assert ev.token_out == "0x4200000000000000000000000000000000000006"
    assert ev.amount_out == 1000
    assert ev.block_number == 0x2AFEC00


def test_validate_event(adapter: EVMAdapter):
    """Events produced by the adapter should pass schema validation."""
    log = {
        "address": "0x4200000000000000000000000000000000000006",
        "blockHash": "0x1234",
        "blockNumber": "0x2afec00",
        "data": "0x00000000000000000000000000000000000000000000000000000000000003e8",
        "logIndex": "0x0",
        "removed": False,
        "topics": [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
            "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2",
        ],
        "transactionHash": "0xdeadbeef",
        "transactionIndex": "0x0",
    }
    adapter._block_ts[0x2AFEC00] = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ev = adapter._decode_log(log)
    assert adapter.validate(ev) is True
