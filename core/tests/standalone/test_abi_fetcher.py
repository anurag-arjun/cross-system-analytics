"""Tests for the ABI fetcher orchestration.

Network clients are mocked: a fake RPC returns canned bytecode + storage
slot values, and fake Etherscan/Sourcify clients drive the fallback path.
An opt-in integration test runs against the real Etherscan V2 API if
ETHERSCAN_API_KEY is set.
"""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

from core.registry.abi_fetcher import (
    ABIFetcher,
    EIP1967_IMPL_SLOT,
    FetchStats,
    _hash_bytecode,
    _parse_impl_address,
)
from core.registry.abi_store import (
    ABIRecord,
    BytecodeRecord,
    InMemoryABIStore,
    InMemoryBytecodeStore,
)
from core.registry.etherscan import (
    EtherscanV2Client,
    NotVerified,
)
from core.registry.sourcify import NotFound


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_hash_bytecode_empty():
    # keccak("") in eth-utils
    expected = "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
    assert _hash_bytecode("0x") == expected
    assert _hash_bytecode("") == expected


def test_hash_bytecode_known():
    code = "0x6080604052"
    h = _hash_bytecode(code)
    assert len(h) == 66
    # Same input -> same hash
    assert h == _hash_bytecode(code)
    assert h != _hash_bytecode("0x6080604053")


def test_parse_impl_address():
    zero = "0x0000000000000000000000000000000000000000000000000000000000000000"
    nonzero = "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert _parse_impl_address(zero) is None
    assert _parse_impl_address("") is None
    assert _parse_impl_address("0x") is None
    assert _parse_impl_address(nonzero) == "0x" + "aa" * 20


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeRPC:
    def __init__(self, codes: dict, storage: dict | None = None):
        self._codes = codes  # {(chain, address): code_hex}
        self._storage = storage or {}
        self.calls = 0

    def get_code(self, chain: str, address: str) -> str:
        self.calls += 1
        key = (chain.lower(), address.lower())
        if key not in self._codes:
            raise RuntimeError(f"no canned bytecode for {chain}/{address}")
        return self._codes[key]

    def get_storage_at(self, chain: str, address: str, slot: str) -> str:
        self.calls += 1
        return self._storage.get(
            (chain.lower(), address.lower(), slot.lower()),
            "0x" + "0" * 64,
        )

    def close(self) -> None:
        pass


class FakeEtherscan:
    def __init__(self, abis: dict | None = None, not_verified: set | None = None):
        self._abis = abis or {}
        self._not_verified = not_verified or set()
        self.calls = 0

    def get_abi(self, chain: str, address: str) -> str:
        self.calls += 1
        key = (chain.lower(), address.lower())
        if key in self._not_verified:
            raise NotVerified(f"{chain}/{address}: not verified")
        if key in self._abis:
            return self._abis[key]
        raise NotVerified(f"{chain}/{address}: not in fake")

    def close(self) -> None:
        pass


class FakeSourcify:
    def __init__(self, abis: dict | None = None):
        self._abis = abis or {}
        self.calls = 0

    def get_abi(self, chainid: int, address: str) -> str:
        self.calls += 1
        key = (chainid, address.lower())
        if key in self._abis:
            return self._abis[key]
        raise NotFound(f"{chainid}/{address}")

    def close(self) -> None:
        pass


def _make_fetcher(rpc: FakeRPC, escan: Any, scify: Any) -> ABIFetcher:
    f = ABIFetcher(
        abi_store=InMemoryABIStore(),
        bytecode_store=InMemoryBytecodeStore(),
        etherscan=escan,
        sourcify=scify,
    )
    f._rpc = rpc  # type: ignore[assignment]
    return f


# ---------------------------------------------------------------------------
# ensure_abi flows
# ---------------------------------------------------------------------------


def test_ensure_abi_happy_path_etherscan():
    chain, addr = "base", "0x" + "11" * 20
    abi = '[{"type":"event","name":"Swap","inputs":[]}]'
    fetcher = _make_fetcher(
        FakeRPC(codes={(chain, addr): "0xdeadbeef"}),
        FakeEtherscan(abis={(chain, addr): abi}),
        FakeSourcify(),
    )
    stats = FetchStats()
    got = fetcher.ensure_abi(chain, addr, stats)
    assert got == abi
    assert stats.abis_fetched == 1
    assert stats.cache_hits == 0
    assert stats.proxies_detected == 0
    assert stats.errors == 0


def test_ensure_abi_cache_hit_on_second_call():
    chain, addr = "base", "0x" + "22" * 20
    abi = '[{"type":"event","name":"X"}]'
    fetcher = _make_fetcher(
        FakeRPC(codes={(chain, addr): "0xcafebabe"}),
        FakeEtherscan(abis={(chain, addr): abi}),
        FakeSourcify(),
    )
    stats = FetchStats()
    fetcher.ensure_abi(chain, addr, stats)
    fetcher.ensure_abi(chain, addr, stats)  # second call hits both caches
    assert stats.cache_hits == 1
    assert stats.abis_fetched == 1
    # Bytecode cached -> no second eth_getCode
    assert fetcher._bc_store.count() == 1


def test_ensure_abi_dedup_by_bytecode_across_addresses():
    """Two addresses with identical bytecode share one ABI cache entry —
    only one Etherscan call should happen."""
    chain = "ethereum"
    a1 = "0x" + "33" * 20
    a2 = "0x" + "44" * 20
    bytecode = "0xfeedface"
    abi = '[{"type":"event","name":"Y"}]'

    escan = FakeEtherscan(abis={(chain, a1): abi})
    fetcher = _make_fetcher(
        FakeRPC(codes={(chain, a1): bytecode, (chain, a2): bytecode}),
        escan,
        FakeSourcify(),
    )
    stats = FetchStats()
    assert fetcher.ensure_abi(chain, a1, stats) == abi
    assert fetcher.ensure_abi(chain, a2, stats) == abi
    assert escan.calls == 1, "second address should hit the bytecode-keyed ABI cache"
    assert stats.abis_fetched == 1
    assert stats.cache_hits == 1


def test_ensure_abi_falls_back_to_sourcify_when_not_verified():
    chain, addr = "polygon", "0x" + "55" * 20
    abi = '[{"type":"event","name":"Z"}]'
    fetcher = _make_fetcher(
        FakeRPC(codes={(chain, addr): "0x6080604052"}),
        FakeEtherscan(not_verified={(chain, addr)}),
        FakeSourcify(abis={(137, addr): abi}),
    )
    stats = FetchStats()
    got = fetcher.ensure_abi(chain, addr, stats)
    assert got == abi
    assert stats.not_verified == 1
    assert stats.sourcify_hits == 1


def test_ensure_abi_returns_none_when_neither_has_it():
    chain, addr = "polygon", "0x" + "66" * 20
    fetcher = _make_fetcher(
        FakeRPC(codes={(chain, addr): "0x6080604053"}),
        FakeEtherscan(not_verified={(chain, addr)}),
        FakeSourcify(),
    )
    stats = FetchStats()
    assert fetcher.ensure_abi(chain, addr, stats) is None
    assert stats.not_verified == 1
    assert stats.sourcify_hits == 0


def test_ensure_abi_eip1967_proxy_uses_implementation_hash_for_dedup():
    chain = "ethereum"
    proxy_addr = "0x" + "77" * 20
    impl_addr = "0x" + "88" * 20
    proxy_code = "0x6080604052aa"
    impl_code = "0x6080604052bb"
    impl_abi = '[{"type":"event","name":"Implementation"}]'

    storage_value = (
        "0x000000000000000000000000" + impl_addr[2:]
    )
    rpc = FakeRPC(
        codes={(chain, proxy_addr): proxy_code, (chain, impl_addr): impl_code},
        storage={(chain, proxy_addr, EIP1967_IMPL_SLOT.lower()): storage_value},
    )
    fetcher = _make_fetcher(
        rpc,
        FakeEtherscan(abis={(chain, impl_addr): impl_abi}),
        FakeSourcify(),
    )
    stats = FetchStats()
    got = fetcher.ensure_abi(chain, proxy_addr, stats)
    assert got == impl_abi
    assert stats.proxies_detected == 1
    rec = fetcher._bc_store.get(chain, proxy_addr)
    assert rec is not None
    assert rec.is_proxy is True
    assert rec.implementation_address == impl_addr
    # The cache key is the implementation's bytecode hash
    assert rec.code_hash == _hash_bytecode(impl_code)


def test_ensure_abi_proxy_with_unfetchable_impl_falls_back_to_proxy_hash():
    """If reading the impl bytecode fails, we still index the address using
    the proxy's bytecode hash and treat it as non-proxy."""
    chain = "ethereum"
    proxy_addr = "0x" + "99" * 20
    impl_addr = "0x" + "aa" * 20
    proxy_code = "0x60806040cc"
    storage_value = "0x000000000000000000000000" + impl_addr[2:]

    # Note: only proxy bytecode is in the canned RPC; impl_addr is missing
    rpc = FakeRPC(
        codes={(chain, proxy_addr): proxy_code},
        storage={(chain, proxy_addr, EIP1967_IMPL_SLOT.lower()): storage_value},
    )
    fetcher = _make_fetcher(
        rpc,
        FakeEtherscan(abis={(chain, proxy_addr): '[{"type":"function","name":"upgradeTo"}]'}),
        FakeSourcify(),
    )
    stats = FetchStats()
    fetcher.ensure_abi(chain, proxy_addr, stats)
    rec = fetcher._bc_store.get(chain, proxy_addr)
    assert rec is not None
    assert rec.is_proxy is False
    assert rec.code_hash == _hash_bytecode(proxy_code)


def test_ensure_many_aggregates_stats():
    chain = "base"
    a1, a2 = "0x" + "11" * 20, "0x" + "22" * 20
    fetcher = _make_fetcher(
        FakeRPC(codes={(chain, a1): "0x01", (chain, a2): "0x02"}),
        FakeEtherscan(abis={(chain, a1): "[]", (chain, a2): "[]"}),
        FakeSourcify(),
    )
    stats = fetcher.ensure_many([(chain, a1), (chain, a2)])
    assert stats.addresses_processed == 2
    assert stats.abis_fetched == 2


# ---------------------------------------------------------------------------
# Real Etherscan smoke (opt-in)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not os.environ.get("ETHERSCAN_API_KEY"),
    reason="No ETHERSCAN_API_KEY",
)
def test_real_etherscan_fetches_weth_abi():
    """One real call to confirm the V2 unified API still works as expected.

    Skipped if the local resolver can't reach api.etherscan.io (some Linux
    setups with systemd-resolved + cached negative entries return NXDOMAIN
    for this host while other domains resolve fine — out of scope for the
    fetcher to fix).
    """
    import socket
    try:
        socket.gethostbyname("api.etherscan.io")
    except socket.gaierror:
        pytest.skip("api.etherscan.io not resolvable in this environment")

    from core.registry.etherscan import EtherscanError

    weth = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    try:
        with EtherscanV2Client(os.environ["ETHERSCAN_API_KEY"]) as client:
            abi = client.get_abi("ethereum", weth)
    except EtherscanError as e:
        if "Name or service not known" in str(e) or "transport error" in str(e):
            pytest.skip(f"Etherscan unreachable: {e}")
        raise
    assert isinstance(abi, str)
    parsed = json.loads(abi)
    assert isinstance(parsed, list)
    assert any(item.get("name") == "Transfer" for item in parsed)
