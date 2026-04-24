"""ClickHouse sink for canonical events."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterator, Protocol

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from core.adapters.base import CanonicalEvent


class EventSink(Protocol):
    def write(self, events: list[CanonicalEvent]) -> int: ...

    def flush(self) -> int: ...

    def close(self) -> None: ...


@dataclass
class SinkConfig:
    host: str = "localhost"
    port: int = 8124
    username: str = "default"
    password: str = "nexus"
    database: str = "default"
    table: str = "canonical_events"
    batch_size: int = 10_000
    auto_flush_interval_sec: float | None = None


class ClickHouseSink(EventSink):
    """Batch writer for canonical_events table.

    Buffers events in memory and flushes to ClickHouse when batch_size
    is reached or flush() is called explicitly.
    """

    def __init__(self, config: SinkConfig | None = None, client: Client | None = None) -> None:
        self.config = config or SinkConfig()
        self._client = client
        self._buffer: list[CanonicalEvent] = []
        self._total_written = 0

    def _ensure_client(self) -> Client:
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                database=self.config.database,
            )
        return self._client

    def write(self, events: list[CanonicalEvent]) -> int:
        """Buffer events. Flush if batch_size exceeded."""
        self._buffer.extend(events)
        if len(self._buffer) >= self.config.batch_size:
            return self.flush()
        return 0

    def write_single(self, event: CanonicalEvent) -> int:
        """Buffer a single event."""
        return self.write([event])

    def flush(self) -> int:
        """Insert buffered events into ClickHouse."""
        if not self._buffer:
            return 0
        client = self._ensure_client()
        rows = [_event_to_row(ev) for ev in self._buffer]
        client.insert(self.config.table, rows)
        count = len(self._buffer)
        self._total_written += count
        self._buffer.clear()
        return count

    def close(self) -> None:
        self.flush()
        if self._client is not None:
            self._client.close()

    @property
    def buffered(self) -> list[CanonicalEvent]:
        return list(self._buffer)

    @property
    def total_written(self) -> int:
        return self._total_written


class InMemorySink(EventSink):
    """Sink that stores events in a list for testing."""

    def __init__(self) -> None:
        self.events: list[CanonicalEvent] = []

    def write(self, events: list[CanonicalEvent]) -> int:
        self.events.extend(events)
        return len(events)

    def flush(self) -> int:
        return 0

    def close(self) -> None:
        pass


class RawLogSink:
    """Batch writer for canonical_logs table (raw EVM logs)."""

    def __init__(self, config: SinkConfig | None = None, client: Client | None = None) -> None:
        self.config = config or SinkConfig(table="canonical_logs")
        self._client = client
        self._buffer: list[dict[str, Any]] = []
        self._total_written = 0

    def _ensure_client(self) -> Client:
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                database=self.config.database,
            )
        return self._client

    def write(self, rows: list[dict[str, Any]]) -> int:
        self._buffer.extend(rows)
        if len(self._buffer) >= self.config.batch_size:
            return self.flush()
        return 0

    def flush(self) -> int:
        if not self._buffer:
            return 0
        client = self._ensure_client()
        rows = [_raw_log_to_row(r) for r in self._buffer]
        client.insert(self.config.table, rows)
        count = len(self._buffer)
        self._total_written += count
        self._buffer.clear()
        return count

    def close(self) -> None:
        self.flush()
        if self._client is not None:
            self._client.close()

    @property
    def total_written(self) -> int:
        return self._total_written


def _event_to_row(ev: CanonicalEvent) -> list[Any]:
    """Convert CanonicalEvent to ClickHouse row tuple."""
    extra_json = json.dumps(ev.extra) if ev.extra is not None else "{}"
    return [
        ev.entity_id or "",
        ev.entity_type or "",
        ev.event_id or "",
        ev.event_type or "",
        ev.event_category or "",
        ev.timestamp,
        ev.source_system or "",
        ev.source_event_id or "",
        ev.chain or "",
        ev.block_number,
        ev.block_time,
        ev.tx_hash,
        ev.log_index,
        ev.protocol or "",
        ev.venue or "",
        ev.token_in,
        ev.token_out,
        ev.amount_in,
        ev.amount_out,
        ev.amount_in_usd,
        ev.amount_out_usd,
        ev.counterparty,
        ev.aggregator or "",
        ev.link_key,
        ev.link_key_type,
        extra_json,
    ]


def _raw_log_to_row(row: dict[str, Any]) -> list[Any]:
    """Convert raw log dict to canonical_logs row tuple."""
    return [
        row["source_system"],
        row["chain"],
        row["block_number"],
        row.get("block_time"),
        row["tx_hash"],
        row["log_index"],
        row["address"],
        row.get("topic0"),
        row.get("topic1"),
        row.get("topic2"),
        row.get("topic3"),
        row["data"],
        row.get("decoded", 0),
        row.get("decoder_version", 0),
        row.get("inserted_at", datetime.now()),
    ]


class BridgeLinkSink:
    """Batch writer for bridge_links table."""

    def __init__(self, config: SinkConfig | None = None, client: Client | None = None) -> None:
        if config is None:
            config = SinkConfig(table="bridge_links")
        else:
            config.table = "bridge_links"
        self.config = config
        self._client = client
        self._buffer: list[Any] = []
        self._total_written = 0

    def _ensure_client(self) -> Client:
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                database=self.config.database,
            )
        return self._client

    def write(self, links: list[Any]) -> int:
        self._buffer.extend(links)
        if len(self._buffer) >= self.config.batch_size:
            return self.flush()
        return 0

    def flush(self) -> int:
        if not self._buffer:
            return 0
        client = self._ensure_client()
        rows = [_bridge_link_to_row(link) for link in self._buffer]
        client.insert(self.config.table, rows)
        count = len(self._buffer)
        self._total_written += count
        self._buffer.clear()
        return count

    def close(self) -> None:
        self.flush()
        if self._client is not None:
            self._client.close()

    @property
    def total_written(self) -> int:
        return self._total_written


def _bridge_link_to_row(link: Any) -> list[Any]:
    """Convert BridgeLink to bridge_links row tuple."""
    return [
        link.link_key,
        link.link_key_type,
        link.src_chain,
        link.src_block_time,
        link.src_tx_hash,
        link.src_entity_id,
        link.src_event_id,
        link.dst_chain,
        link.dst_block_time,
        link.dst_tx_hash,
        link.dst_entity_id,
        link.dst_event_id,
        link.token,
        link.amount,
        None,  # amount_usd (enriched later)
        1.0,  # link_confidence
        datetime.now(),
    ]
