"""Standalone test for bridge decoders — fetches real bridge events + unit tests.

Requirements: HYPERSYNC_TOKEN in .env
"""

from datetime import datetime, timedelta, timezone

import pytest

from core.adapters.evm import EVMAdapter
from core.adapters.evm.decoders.bridge import (
    ArbitrumDepositInitiatedDecoder,
    ArbitrumOutBoxTransactionExecutedDecoder,
    ArbitrumWithdrawalFinalizedDecoder,
)

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


class TestArbitrumBridgeDecoders:
    """Unit tests for Arbitrum canonical bridge decoders using mock logs."""

    def _make_log(self, topic0: str, indexed: list[str], data: str) -> dict:
        """Construct a mock log dict matching HyperSync format."""
        return {
            "address": "0xa3a7b6f61261ade96077ba6b56befcc25fa7e4ba",
            "topics": [topic0] + indexed,
            "data": data,
            "blockNumber": "0x1234567",
            "transactionHash": "0x" + "ab" * 32,
            "logIndex": "0x1",
        }

    def test_deposit_initiated_decode(self):
        from eth_abi import encode

        decoder = ArbitrumDepositInitiatedDecoder()

        # Indexed: l1Token, l2Token, from
        l1_token = "0x" + "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        l2_token = "0x" + "af88d065e77c8cc2239327c5edb3a432268e5831"
        sender = "0x" + "ab" * 20  # 20 bytes = 40 hex chars

        # Pad indexed params to 32 bytes for topics
        def pad32(addr: str) -> str:
            return "0x" + addr[2:].rjust(64, "0")

        indexed = [pad32(l1_token), pad32(l2_token), pad32(sender)]

        # Data: to (address), sequenceNumber (uint256), amount (uint256)
        receiver = "0x" + "cd" * 20
        data = "0x" + encode(
            ["address", "uint256", "uint256"],
            [receiver, 42, 1000000000000000000],  # 1 ETH
        ).hex()

        log = self._make_log(decoder.topic0, indexed, data)
        ev = decoder.decode(log, datetime.now(timezone.utc))

        assert ev is not None
        assert ev.event_type == "bridge_out"
        assert ev.protocol == "arbitrum_bridge"
        assert ev.entity_id == sender
        assert ev.counterparty == receiver
        assert ev.token_in == l1_token
        assert ev.token_out == l2_token
        assert ev.amount_out == 1000000000000000000
        assert ev.link_key == "42"
        assert ev.link_key_type == "arbitrum_sequence"

    def test_withdrawal_finalized_decode(self):
        from eth_abi import encode

        decoder = ArbitrumWithdrawalFinalizedDecoder()

        l1_token = "0x" + "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        l2_token = "0x" + "af88d065e77c8cc2239327c5edb3a432268e5831"
        sender = "0x" + "ab" * 20

        def pad32(addr: str) -> str:
            return "0x" + addr[2:].rjust(64, "0")

        indexed = [pad32(l1_token), pad32(l2_token), pad32(sender)]

        receiver = "0x" + "cd" * 20
        data = "0x" + encode(
            ["address", "uint256", "uint256"],
            [receiver, 17, 500000000],  # 500 USDC (6 decimals)
        ).hex()

        log = self._make_log(decoder.topic0, indexed, data)
        ev = decoder.decode(log, datetime.now(timezone.utc))

        assert ev is not None
        assert ev.event_type == "bridge_in"
        assert ev.protocol == "arbitrum_bridge"
        assert ev.entity_id == receiver
        assert ev.counterparty == sender
        assert ev.token_out == l1_token
        assert ev.amount_in == 500000000
        assert ev.link_key == "17"
        assert ev.link_key_type == "arbitrum_exit_num"

    def test_outbox_transaction_executed_decode(self):
        from eth_abi import encode

        decoder = ArbitrumOutBoxTransactionExecutedDecoder()

        to_addr = "0x" + "cd" * 20
        l2_sender = "0x" + "ef" * 20
        index = 3

        def pad32(val: str | int) -> str:
            if isinstance(val, int):
                return "0x" + format(val, "064x")
            return "0x" + val[2:].rjust(64, "0")

        indexed = [pad32(to_addr), pad32(l2_sender), pad32(index)]

        # Data: txNum (uint256)
        data = "0x" + encode(["uint256"], [12345]).hex()

        log = self._make_log(decoder.topic0, indexed, data)
        ev = decoder.decode(log, datetime.now(timezone.utc))

        assert ev is not None
        assert ev.event_type == "bridge_in"
        assert ev.protocol == "arbitrum_bridge"
        assert ev.entity_id == to_addr
        assert ev.counterparty == l2_sender
        assert ev.link_key == "3"
        assert ev.link_key_type == "arbitrum_outbox_index"
        assert ev.extra["tx_num"] == "12345"
        assert ev.extra["l2_sender"] == l2_sender
