"""EVM chain adapter with HyperSync primary and JSON-RPC fallback."""

from __future__ import annotations

import asyncio
import hashlib
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterator

import httpx
import hypersync
from dotenv import load_dotenv

from core.adapters.base import Adapter, CanonicalEvent
from core.adapters.evm.decoders import DecodedEvent, LogDecoder
from core.adapters.evm.registry import DecoderRegistry, build_default_registry

load_dotenv()


class JsonRpcClient:
    def __init__(self, url: str, chain: str, timeout: float = 120.0) -> None:
        self.url = url
        self.chain = chain
        self._client = httpx.Client(timeout=timeout)
        self._req_id = 0

    def _call(self, method: str, params: list[Any] | dict[str, Any]) -> Any:
        self._req_id += 1
        payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": self._req_id}
        resp = self._client.post(self.url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"JSON-RPC error: {data['error']}")
        return data["result"]

    def get_block_number(self) -> int:
        return int(self._call("eth_blockNumber", []), 16)

    def get_logs(
        self,
        from_block: int,
        to_block: int,
        address: list[str] | str | None = None,
        topics: list[list[str] | str | None] | None = None,
    ) -> list[dict]:
        params: dict[str, Any] = {"fromBlock": hex(from_block), "toBlock": hex(to_block)}
        if address is not None:
            params["address"] = address
        if topics is not None:
            params["topics"] = topics
        return self._call("eth_getLogs", [params])

    def get_block_by_number(self, block_num: int) -> dict:
        return self._call("eth_getBlockByNumber", [hex(block_num), False])

    def close(self) -> None:
        self._client.close()


def _raw_log_row(source_system: str, log: dict, ts: datetime | None) -> dict[str, Any]:
    topics = log.get("topics", [])
    return {
        "source_system": source_system,
        "chain": source_system.replace("evm_", ""),
        "block_number": int(log["blockNumber"], 16),
        "block_time": ts,
        "tx_hash": log["transactionHash"],
        "log_index": int(log["logIndex"], 16),
        "address": log["address"],
        "topic0": topics[0] if len(topics) > 0 else None,
        "topic1": topics[1] if len(topics) > 1 else None,
        "topic2": topics[2] if len(topics) > 2 else None,
        "topic3": topics[3] if len(topics) > 3 else None,
        "data": log.get("data", "0x"),
    }


class EVMAdapter(Adapter):
    """Ingest EVM logs via JSON-RPC (fallback) or HyperSync (preferred).

    Two modes of operation:
    1. **Full-logs** (`ingest_raw`): Fetch every log in a block range. Store
       raw topics + data for later decoding. Enables emergent protocol detection.
    2. **Filtered** (`ingest`): Fetch only logs matching known decoders in the
       registry. Fast, but misses unknown protocols.
    """

    @property
    def source_system(self) -> str:
        return f"evm_{self.chain}"

    def __init__(
        self,
        chain: str,
        rpc_url: str | None = None,
        hyper_token: str | None = None,
        hyper_url: str | None = None,
        page_size: int = 2_000,
        registry: DecoderRegistry | None = None,
    ) -> None:
        self.chain = chain
        self.hyper_token = hyper_token or os.getenv("HYPERSYNC_TOKEN")
        self.hyper_url = hyper_url or f"https://{chain}.hypersync.xyz"
        self.page_size = page_size
        self.registry = registry or build_default_registry()

        self.rpc_url = rpc_url or self._default_rpc(chain)
        self._rpc: JsonRpcClient | None = None
        self._hyper: hypersync.HypersyncClient | None = None
        self._block_ts: dict[int, datetime] = {}

    def _default_rpc(self, chain: str) -> str:
        return {
            "base": "https://base-rpc.publicnode.com",
            "ethereum": "https://ethereum-rpc.publicnode.com",
            "arbitrum": "https://arbitrum-one-rpc.publicnode.com",
            "optimism": "https://optimism-rpc.publicnode.com",
        }.get(chain, "https://base-rpc.publicnode.com")

    def _ensure_rpc(self) -> JsonRpcClient:
        if self._rpc is None:
            self._rpc = JsonRpcClient(self.rpc_url, self.chain)
        return self._rpc

    def _ensure_hyper(self) -> hypersync.HypersyncClient:
        if self._hyper is None:
            cfg = hypersync.ClientConfig(
                url=self.hyper_url,
                bearer_token=self.hyper_token,
            )
            self._hyper = hypersync.HypersyncClient(cfg)
        return self._hyper

    def _block_time(self, block_number: int) -> datetime:
        if block_number not in self._block_ts:
            rpc = self._ensure_rpc()
            blk = rpc.get_block_by_number(block_number)
            ts = int(blk["timestamp"], 16)
            self._block_ts[block_number] = datetime.fromtimestamp(ts, tz=timezone.utc)
        return self._block_ts[block_number]

    # ------------------------------------------------------------------
    # Full-logs mode: fetch everything, decode later
    # ------------------------------------------------------------------

    def ingest_raw(
        self,
        start: datetime,
        end: datetime,
        addresses: list[str] | str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Fetch ALL logs (no topic filter) as raw rows for `canonical_logs`."""
        if self.hyper_token:
            yield from self._ingest_raw_hypersync(start, end, addresses)
        else:
            yield from self._ingest_raw_jsonrpc(start, end, addresses)

    def _ingest_raw_jsonrpc(
        self,
        start: datetime,
        end: datetime,
        addresses: list[str] | str | None = None,
    ) -> Iterator[dict[str, Any]]:
        rpc = self._ensure_rpc()
        head = rpc.get_block_number()
        secs_per_block = 2 if self.chain in ("base", "arbitrum", "optimism") else 12
        start_block = max(
            0, head - int((datetime.now(timezone.utc) - start).total_seconds() / secs_per_block)
        )
        end_block = head - int((datetime.now(timezone.utc) - end).total_seconds() / secs_per_block)
        end_block = max(start_block, end_block)

        for from_blk in range(start_block, end_block + 1, self.page_size):
            to_blk = min(from_blk + self.page_size - 1, end_block)
            logs = rpc.get_logs(from_blk, to_blk, address=addresses)
            for log in logs:
                block_number = int(log["blockNumber"], 16)
                ts = self._block_time(block_number)
                yield _raw_log_row(self.source_system, log, ts)

    def _ingest_raw_hypersync(
        self,
        start: datetime,
        end: datetime,
        addresses: list[str] | str | None = None,
    ) -> Iterator[dict[str, Any]]:
        return iter(asyncio.run(self._ingest_raw_hypersync_async(start, end, addresses)))

    async def _ingest_raw_hypersync_async(
        self,
        start: datetime,
        end: datetime,
        addresses: list[str] | str | None = None,
    ) -> list[dict[str, Any]]:
        client = self._ensure_hyper()
        head = await client.get_height()

        secs_per_block = 2 if self.chain in ("base", "arbitrum", "optimism") else 12
        start_block = max(
            0, head - int((datetime.now(timezone.utc) - start).total_seconds() / secs_per_block)
        )
        end_block = head - int((datetime.now(timezone.utc) - end).total_seconds() / secs_per_block)
        end_block = max(start_block, end_block)

        log_selection = hypersync.LogSelection()
        if addresses is not None:
            addr_list = [addresses] if isinstance(addresses, str) else addresses
            log_selection.address = addr_list

        query = hypersync.Query(
            from_block=start_block,
            to_block=end_block,
            logs=[log_selection],
            field_selection=hypersync.FieldSelection(
                log=[
                    "address",
                    "topic0",
                    "topic1",
                    "topic2",
                    "topic3",
                    "data",
                    "block_number",
                    "transaction_hash",
                    "log_index",
                ],
                block=["number", "timestamp"],
            ),
        )

        results: list[dict[str, Any]] = []
        block_ts_map: dict[int, datetime] = {}

        while True:
            resp = await client.get(query)
            for log in resp.data.logs:
                block_number = log.block_number
                if block_number not in block_ts_map:
                    for blk in resp.data.blocks:
                        if blk.number == block_number:
                            ts_hex = blk.timestamp
                            ts = int(ts_hex, 16) if isinstance(ts_hex, str) else int(ts_hex)
                            block_ts_map[block_number] = datetime.fromtimestamp(ts, tz=timezone.utc)
                            break
                    else:
                        block_ts_map[block_number] = self._block_time(block_number)
                ts = block_ts_map[block_number]
                results.append(
                    _raw_log_row(
                        self.source_system,
                        {
                            "address": log.address,
                            "topics": log.topics,
                            "data": log.data,
                            "blockNumber": hex(log.block_number),
                            "transactionHash": log.transaction_hash,
                            "logIndex": hex(log.log_index),
                        },
                        ts,
                    )
                )

            if resp.next_block >= end_block:
                break
            query.from_block = resp.next_block

        return results

    # ------------------------------------------------------------------
    # Decode pipeline: raw logs -> canonical_events
    # ------------------------------------------------------------------

    def decode_logs(self, raw_logs: list[dict[str, Any]]) -> Iterator[CanonicalEvent]:
        """Decode a batch of raw logs using the current registry."""
        for row in raw_logs:
            topic0 = row.get("topic0")
            decoder = self.registry.lookup(topic0, row.get("address"))
            if decoder is None:
                continue
            log = {
                "address": row["address"],
                "topics": [
                    t
                    for t in [
                        row.get("topic0"),
                        row.get("topic1"),
                        row.get("topic2"),
                        row.get("topic3"),
                    ]
                    if t
                ],
                "data": row["data"],
                "blockNumber": hex(row["block_number"]),
                "transactionHash": row["tx_hash"],
                "logIndex": hex(row["log_index"]),
            }
            ts = row.get("block_time") or self._block_time(row["block_number"])
            decoded = decoder.decode(log, ts)
            if decoded is None:
                continue
            yield self._to_canonical(decoded)

    def unknown_topic0s(self, raw_logs: list[dict[str, Any]]) -> dict[str, int]:
        """Count how many logs have topic0s not in the current registry."""
        counts: dict[str, int] = {}
        for row in raw_logs:
            t0 = row.get("topic0")
            if t0 and self.registry.lookup(t0, row.get("address")) is None:
                counts[t0] = counts.get(t0, 0) + 1
        return counts

    # ------------------------------------------------------------------
    # Filtered mode (original): fetch only known topic0s
    # ------------------------------------------------------------------

    def ingest(
        self,
        start: datetime,
        end: datetime,
        addresses: list[str] | str | None = None,
    ) -> Iterator[CanonicalEvent]:
        if self.hyper_token:
            yield from self._ingest_hypersync(start, end, addresses)
        else:
            yield from self._ingest_jsonrpc(start, end, addresses)

    def _ingest_jsonrpc(
        self,
        start: datetime,
        end: datetime,
        addresses: list[str] | str | None = None,
    ) -> Iterator[CanonicalEvent]:
        rpc = self._ensure_rpc()
        head = rpc.get_block_number()
        secs_per_block = 2 if self.chain in ("base", "arbitrum", "optimism") else 12
        start_block = max(
            0, head - int((datetime.now(timezone.utc) - start).total_seconds() / secs_per_block)
        )
        end_block = head - int((datetime.now(timezone.utc) - end).total_seconds() / secs_per_block)
        end_block = max(start_block, end_block)

        topics = [self.registry.all_topic0s()]
        for from_blk in range(start_block, end_block + 1, self.page_size):
            to_blk = min(from_blk + self.page_size - 1, end_block)
            logs = rpc.get_logs(from_blk, to_blk, address=addresses, topics=topics)
            for log in logs:
                topic0 = log.get("topics", [None])[0]
                decoder = self.registry.lookup(topic0, log.get("address"))
                if decoder is None:
                    continue
                block_number = int(log["blockNumber"], 16)
                ts = self._block_time(block_number)
                decoded = decoder.decode(log, ts)
                if decoded is None:
                    continue
                yield self._to_canonical(decoded)

    def _ingest_hypersync(
        self,
        start: datetime,
        end: datetime,
        addresses: list[str] | str | None = None,
    ) -> Iterator[CanonicalEvent]:
        return iter(asyncio.run(self._ingest_hypersync_async(start, end, addresses)))

    async def _ingest_hypersync_async(
        self,
        start: datetime,
        end: datetime,
        addresses: list[str] | str | None = None,
    ) -> list[CanonicalEvent]:
        client = self._ensure_hyper()
        head = await client.get_height()

        secs_per_block = 2 if self.chain in ("base", "arbitrum", "optimism") else 12
        start_block = max(
            0, head - int((datetime.now(timezone.utc) - start).total_seconds() / secs_per_block)
        )
        end_block = head - int((datetime.now(timezone.utc) - end).total_seconds() / secs_per_block)
        end_block = max(start_block, end_block)

        topic0s = self.registry.all_topic0s()
        log_selection = hypersync.LogSelection(topics=[topic0s])
        if addresses is not None:
            addr_list = [addresses] if isinstance(addresses, str) else addresses
            log_selection.address = addr_list

        query = hypersync.Query(
            from_block=start_block,
            to_block=end_block,
            logs=[log_selection],
            field_selection=hypersync.FieldSelection(
                log=[
                    "address",
                    "topic0",
                    "topic1",
                    "topic2",
                    "data",
                    "block_number",
                    "transaction_hash",
                    "log_index",
                ],
                block=["number", "timestamp"],
            ),
        )

        results: list[CanonicalEvent] = []
        block_ts_map: dict[int, datetime] = {}

        while True:
            resp = await client.get(query)
            for log in resp.data.logs:
                topic0 = log.topics[0] if log.topics else None
                decoder = self.registry.lookup(topic0, log.address)
                if decoder is None:
                    continue
                block_number = log.block_number
                if block_number not in block_ts_map:
                    for blk in resp.data.blocks:
                        if blk.number == block_number:
                            ts_hex = blk.timestamp
                            ts = int(ts_hex, 16) if isinstance(ts_hex, str) else int(ts_hex)
                            block_ts_map[block_number] = datetime.fromtimestamp(ts, tz=timezone.utc)
                            break
                    else:
                        block_ts_map[block_number] = self._block_time(block_number)
                ts = block_ts_map[block_number]
                decoded = decoder.decode(
                    {
                        "address": log.address,
                        "topics": log.topics,
                        "data": log.data,
                        "blockNumber": hex(log.block_number),
                        "transactionHash": log.transaction_hash,
                        "logIndex": hex(log.log_index),
                    },
                    ts,
                )
                if decoded is None:
                    continue
                results.append(self._to_canonical(decoded))

            if resp.next_block >= end_block:
                break
            query.from_block = resp.next_block

        return results

    def _to_canonical(self, ev: DecodedEvent) -> CanonicalEvent:
        source_event_id = f"{ev.tx_hash}:{ev.log_index}"
        event_id = hashlib.sha256(f"{self.source_system}:{source_event_id}".encode()).hexdigest()
        return CanonicalEvent(
            entity_id=ev.entity_id,
            entity_type="wallet",
            event_id=event_id,
            event_type=ev.event_type,
            event_category="transaction",
            timestamp=ev.timestamp,
            source_system=self.source_system,
            source_event_id=source_event_id,
            chain=self.chain,
            block_number=ev.block_number,
            block_time=ev.timestamp,
            tx_hash=ev.tx_hash,
            log_index=ev.log_index,
            protocol=ev.protocol or None,
            venue=ev.venue or None,
            token_in=ev.token_in,
            token_out=ev.token_out,
            amount_in=ev.amount_in,
            amount_out=ev.amount_out,
            counterparty=ev.counterparty,
            link_key=ev.link_key,
            link_key_type=ev.link_key_type,
            extra=ev.extra,
        )

    def _decode_log(self, log: dict) -> CanonicalEvent:
        topic0 = log.get("topics", [None])[0]
        decoder = self.registry.lookup(topic0, log.get("address"))
        if decoder is None:
            raise ValueError(f"No decoder for topic0={topic0}")
        block_number = int(log["blockNumber"], 16)
        ts = self._block_time(block_number)
        decoded = decoder.decode(log, ts)
        if decoded is None:
            raise ValueError("Decoding returned None")
        return self._to_canonical(decoded)

    def close(self) -> None:
        if self._rpc is not None:
            self._rpc.close()
