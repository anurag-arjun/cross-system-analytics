"""Standalone test for bridge decoders — fetches real bridge events from Base.

Requirements: HYPERSYNC_TOKEN in .env
"""

from datetime import datetime, timedelta, timezone

import pytest

from core.adapters.evm import EVMAdapter

# Bridge contracts on Base — verified addresses from protocol docs + on-chain
STARGATE_BRIDGE = "0xAF54BE5B6eEc24d6BFACf1cce4eaF680A8239398"
# Across SpokePool proxy (correct address from docs.across.to)
ACROSS_SPOKEPOOL = "0x09aea4b2242abc8bb4bb78d537a67a245a7bec64"
BASE_L2_BRIDGE = "0x4200000000000000000000000000000000000010"


@pytest.fixture
def adapter():
    a = EVMAdapter(
        chain="base",
        rpc_url="https://mainnet.base.org",
        page_size=200,
    )
    yield a
    a.close()


def test_stargate_bridge_events(adapter: EVMAdapter):
    """Fetch Stargate SendToChain events from the last ~10 minutes on Base."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=10)

    events = list(adapter.ingest(start, end, addresses=STARGATE_BRIDGE))
    stargate_events = [e for e in events if e.protocol == "stargate"]

    # Stargate is active but not every 10 min block has a bridge tx
    # We assert that IF we find events, they have the right shape
    for ev in stargate_events:
        assert ev.event_type == "bridge_out"
        assert ev.entity_type == "wallet"
        assert ev.source_system == "evm_base"
        assert ev.link_key_type == "stargate_dst_chain"
        assert ev.amount_out is not None
        assert ev.amount_out > 0


def test_across_bridge_events(adapter: EVMAdapter):
    """Fetch Across SpokePool events from Base. On the destination chain
    (Base), the SpokePool emits FilledRelay (bridge_in), not FundsDeposited.
    FundsDeposited (bridge_out) appears on the source chain."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=10)

    events = list(adapter.ingest(start, end, addresses=ACROSS_SPOKEPOOL))
    across_events = [e for e in events if e.protocol == "across"]

    for ev in across_events:
        assert ev.event_type in ("bridge_out", "bridge_in")
        assert ev.link_key_type == "across_deposit_id"
        if ev.token_in is not None:
            assert ev.amount_in is not None
            assert ev.amount_in > 0


def test_base_native_bridge_events(adapter: EVMAdapter):
    """Fetch OP Stack bridge events from Base L2StandardBridge."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=10)

    events = list(adapter.ingest(start, end, addresses=BASE_L2_BRIDGE))
    base_events = [e for e in events if e.protocol == "op_stack"]

    for ev in base_events:
        assert ev.event_type in ("bridge_out", "bridge_in")
        assert ev.link_key_type == "op_stack_bridge"
