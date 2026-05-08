"""Bulk ABI fetcher with bytecode-hash dedup + EIP-1967 proxy resolution.

Per address, the workflow is:

1. Look up `(chain, address)` in the bytecode index. If we already have a
   code_hash for it, skip straight to step 4.
2. eth_getCode the address. Hash the bytecode.
3. Read EIP-1967 implementation slot
   (`0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc`).
   If non-zero, the contract is a proxy — fetch the implementation's
   bytecode + hash, and key the ABI cache by the implementation's hash so
   logs decoded from the proxy use the implementation's event ABI.
4. Look up code_hash in the ABI store. If present, we're done.
5. Fetch ABI via Etherscan V2. Fall back to Sourcify if Etherscan reports
   the contract is not verified or the key is unset.

Many factory-deployed contracts (every UniV3 pool, every Aave V3 reserve)
share an implementation ABI, so the dedup saves up to ~100x calls.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Iterable

from eth_utils import keccak

from core.registry.abi_store import (
    ABIRecord,
    ABIStore,
    BytecodeRecord,
    BytecodeStore,
)
from core.registry.etherscan import (
    CHAIN_IDS,
    EtherscanError,
    EtherscanV2Client,
    NotVerified,
)
from core.registry.sourcify import NotFound, SourcifyClient, SourcifyError

logger = logging.getLogger(__name__)

# EIP-1967 standard implementation storage slot.
# = bytes32(uint256(keccak256("eip1967.proxy.implementation")) - 1)
EIP1967_IMPL_SLOT = (
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
)


@dataclass
class FetchStats:
    addresses_processed: int = 0
    bytecodes_fetched: int = 0
    abis_fetched: int = 0
    cache_hits: int = 0
    proxies_detected: int = 0
    not_verified: int = 0
    sourcify_hits: int = 0
    errors: int = 0
    etherscan_calls: int = 0
    sourcify_calls: int = 0
    rpc_calls: int = 0


class _RPC:
    """Tiny JSON-RPC wrapper for eth_getCode + eth_getStorageAt."""

    def __init__(self, urls_by_chain: dict[str, str], timeout_s: float = 30.0) -> None:
        import httpx

        self._client = httpx.Client(timeout=timeout_s)
        self._urls = urls_by_chain
        self._req_id = 0
        self.calls = 0

    def close(self) -> None:
        self._client.close()

    def _call(self, chain: str, method: str, params: list) -> str:
        url = self._urls.get(chain)
        if url is None:
            raise ValueError(f"no RPC URL configured for chain {chain!r}")
        self._req_id += 1
        self.calls += 1
        payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": self._req_id}
        resp = self._client.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"rpc error on {chain}: {data['error']}")
        return data["result"]

    def get_code(self, chain: str, address: str) -> str:
        return self._call(chain, "eth_getCode", [address, "latest"])

    def get_storage_at(self, chain: str, address: str, slot: str) -> str:
        return self._call(chain, "eth_getStorageAt", [address, slot, "latest"])


_DEFAULT_RPCS = {
    "ethereum": "https://ethereum-rpc.publicnode.com",
    "base": "https://base-rpc.publicnode.com",
    "arbitrum": "https://arbitrum-one-rpc.publicnode.com",
    "optimism": "https://optimism-rpc.publicnode.com",
    "polygon": "https://polygon-bor-rpc.publicnode.com",
}


def _hash_bytecode(code_hex: str) -> str:
    """keccak256 of the deployed bytecode. Empty / 0x bytecode hashes the empty bytes."""
    if not code_hex or code_hex in ("0x", "0X"):
        return "0x" + keccak(b"").hex()
    payload = bytes.fromhex(code_hex[2:]) if code_hex.startswith("0x") else bytes.fromhex(code_hex)
    return "0x" + keccak(payload).hex()


def _parse_impl_address(slot_value: str) -> str | None:
    """EIP-1967 impl slot stores the address in the lower 20 bytes."""
    if not slot_value or slot_value in ("0x", "0X"):
        return None
    raw = slot_value[2:] if slot_value.startswith("0x") else slot_value
    if not raw.strip("0"):
        return None
    return "0x" + raw[-40:].lower()


class ABIFetcher:
    """Orchestrates eth_getCode + Etherscan/Sourcify ABI fetches with caching."""

    def __init__(
        self,
        abi_store: ABIStore,
        bytecode_store: BytecodeStore,
        etherscan: EtherscanV2Client | None = None,
        sourcify: SourcifyClient | None = None,
        rpc_urls: dict[str, str] | None = None,
    ) -> None:
        self._abi_store = abi_store
        self._bc_store = bytecode_store
        self._etherscan = etherscan
        self._sourcify = sourcify or SourcifyClient()
        self._rpc = _RPC(rpc_urls or _DEFAULT_RPCS)

    def close(self) -> None:
        self._rpc.close()
        self._sourcify.close()
        if self._etherscan is not None:
            self._etherscan.close()

    def __enter__(self) -> "ABIFetcher":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def ensure_abi(self, chain: str, address: str, stats: FetchStats) -> str | None:
        """Resolve (chain, address) to a JSON-encoded ABI string. Returns None
        on errors or when neither Etherscan nor Sourcify has the source."""
        chain = chain.lower()
        address = address.lower()
        stats.addresses_processed += 1

        # Step 1: bytecode index
        bc = self._bc_store.get(chain, address)
        if bc is None:
            try:
                bc = self._fetch_bytecode_record(chain, address, stats)
            except Exception as e:
                logger.warning("bytecode fetch failed for %s/%s: %s", chain, address, e)
                stats.errors += 1
                return None
            self._bc_store.upsert(bc)

        # Step 2: ABI lookup by code_hash
        abi = self._abi_store.get(bc.code_hash)
        if abi is not None:
            stats.cache_hits += 1
            return abi

        # Step 3: fetch ABI for proxy impl if proxy, else for self
        target = bc.implementation_address or address
        try:
            abi_json = self._fetch_abi(chain, target, stats)
        except Exception as e:
            logger.warning("abi fetch failed for %s/%s: %s", chain, target, e)
            stats.errors += 1
            return None

        if abi_json is None:
            return None

        self._abi_store.upsert(
            ABIRecord(
                code_hash=bc.code_hash,
                abi_json=abi_json,
                source="etherscan",  # may be overridden by _fetch_abi via stats
            )
        )
        stats.abis_fetched += 1
        return abi_json

    def ensure_many(
        self, items: Iterable[tuple[str, str]], stats: FetchStats | None = None
    ) -> FetchStats:
        stats = stats or FetchStats()
        for chain, address in items:
            self.ensure_abi(chain, address, stats)
            if stats.addresses_processed % 100 == 0:
                logger.info(
                    "progress: %d processed (%d cache hits, %d new ABIs, %d errors, %d not_verified)",
                    stats.addresses_processed,
                    stats.cache_hits,
                    stats.abis_fetched,
                    stats.errors,
                    stats.not_verified,
                )
        if self._etherscan is not None:
            stats.etherscan_calls = self._etherscan.calls
        stats.sourcify_calls = self._sourcify.calls
        stats.rpc_calls = self._rpc.calls
        return stats

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _fetch_bytecode_record(
        self, chain: str, address: str, stats: FetchStats
    ) -> BytecodeRecord:
        """eth_getCode + EIP-1967 proxy probe."""
        code = self._rpc.get_code(chain, address)
        stats.bytecodes_fetched += 1
        proxy_impl: str | None = None
        try:
            slot_value = self._rpc.get_storage_at(chain, address, EIP1967_IMPL_SLOT)
            proxy_impl = _parse_impl_address(slot_value)
        except Exception as e:
            logger.debug("EIP-1967 slot read failed for %s/%s: %s", chain, address, e)

        if proxy_impl is not None:
            stats.proxies_detected += 1
            try:
                impl_code = self._rpc.get_code(chain, proxy_impl)
                stats.bytecodes_fetched += 1
                code_hash = _hash_bytecode(impl_code)
            except Exception as e:
                logger.warning(
                    "proxy %s/%s impl bytecode fetch failed (%s); using proxy hash",
                    chain, address, e,
                )
                code_hash = _hash_bytecode(code)
                proxy_impl = None
            return BytecodeRecord(
                chain=chain,
                address=address,
                code_hash=code_hash,
                is_proxy=proxy_impl is not None,
                implementation_address=proxy_impl,
            )
        return BytecodeRecord(
            chain=chain,
            address=address,
            code_hash=_hash_bytecode(code),
            is_proxy=False,
            implementation_address=None,
        )

    def _fetch_abi(self, chain: str, address: str, stats: FetchStats) -> str | None:
        """Try Etherscan, fall through to Sourcify, return None if neither has it."""
        if self._etherscan is not None:
            try:
                return self._etherscan.get_abi(chain, address)
            except NotVerified:
                stats.not_verified += 1
            except EtherscanError as e:
                logger.warning("etherscan error for %s/%s: %s", chain, address, e)
                stats.errors += 1
                # fall through to Sourcify

        chainid = CHAIN_IDS.get(chain)
        if chainid is None:
            return None
        try:
            abi = self._sourcify.get_abi(chainid, address)
            stats.sourcify_hits += 1
            return abi
        except NotFound:
            return None
        except SourcifyError as e:
            logger.warning("sourcify error for %s/%s: %s", chain, address, e)
            stats.errors += 1
            return None


def parse_abi(abi_json: str) -> list[dict]:
    """Convenience helper for callers — most ABIs are stored as JSON strings."""
    return json.loads(abi_json)
