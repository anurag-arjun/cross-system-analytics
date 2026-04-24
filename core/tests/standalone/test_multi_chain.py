"""Tests for MultiChainAdapter parallel ingestion."""

from datetime import datetime, timedelta, timezone

import pytest

from core.adapters.evm.multi import ChainConfig, MultiChainAdapter


class TestMultiChainAdapter:
    def test_initializes_multiple_adapters(self):
        adapter = MultiChainAdapter(
            [
                ChainConfig("base", rpc_url="https://mainnet.base.org"),
                ChainConfig("ethereum", rpc_url="https://ethereum-rpc.publicnode.com"),
            ]
        )

        assert len(adapter.adapters) == 2
        assert "base" in adapter.adapters
        assert "ethereum" in adapter.adapters
        adapter.close()

    def test_sequential_ingest(self):
        adapter = MultiChainAdapter(
            [
                ChainConfig("base", rpc_url="https://mainnet.base.org", page_size=200),
            ]
        )

        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=1)

        events = list(
            adapter.ingest(start, end, addresses="0x4200000000000000000000000000000000000006")
        )
        assert len(events) > 0
        for ev in events:
            assert ev.chain == "base"

        adapter.close()

    def test_raw_ingest(self):
        adapter = MultiChainAdapter(
            [
                ChainConfig("base", rpc_url="https://mainnet.base.org", page_size=200),
            ]
        )

        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=1)

        logs = list(
            adapter.ingest_raw(start, end, addresses="0x4200000000000000000000000000000000000006")
        )
        assert len(logs) > 0
        for log in logs:
            assert log["chain"] == "base"

        adapter.close()

    @pytest.mark.asyncio
    async def test_async_ingest(self):
        adapter = MultiChainAdapter(
            [
                ChainConfig("base", rpc_url="https://mainnet.base.org", page_size=200),
            ]
        )

        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=1)

        events = []
        async for ev in adapter.ingest_async(
            start, end, addresses="0x4200000000000000000000000000000000000006"
        ):
            events.append(ev)

        assert len(events) > 0
        adapter.close()

    def test_events_tagged_with_chain(self):
        adapter = MultiChainAdapter(
            [
                ChainConfig("base", rpc_url="https://mainnet.base.org", page_size=200),
            ]
        )

        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=1)

        events = list(
            adapter.ingest(start, end, addresses="0x4200000000000000000000000000000000000006")
        )
        assert len(events) > 0

        chains = {ev.chain for ev in events}
        assert chains == {"base"}

        adapter.close()
