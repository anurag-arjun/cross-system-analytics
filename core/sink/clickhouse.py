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
    database: str = "nexus"
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
        """Buffer events.  Deduplication is deferred to flush() to avoid
        a ClickHouse round-trip on every single-event write."""
        self._buffer.extend(events)
        if len(self._buffer) >= self.config.batch_size:
            return self.flush()
        return 0

    def write_single(self, event: CanonicalEvent) -> int:
        """Buffer a single event."""
        return self.write([event])

    def flush(self) -> int:
        """Deduplicate buffered events against ClickHouse, then insert."""
        if not self._buffer:
            return 0
        client = self._ensure_client()
        new_events = self._deduplicate(self._buffer)
        if not new_events:
            self._buffer.clear()
            return 0
        rows = [_event_to_row(ev) for ev in new_events]
        client.insert(self.config.table, rows)
        count = len(new_events)
        self._total_written += count
        self._buffer.clear()
        return count

    def _deduplicate(self, events: list[CanonicalEvent]) -> list[CanonicalEvent]:
        """Filter out events whose event_id already exists in ClickHouse.

        Queries are chunked to avoid 'Field value too long' errors when the
        IN clause contains thousands of IDs."""
        if not events or self._client is None:
            return events

        event_ids = [ev.event_id for ev in events]
        existing: set[str] = set()

        # Chunk to keep the HTTP query under ClickHouse's field size limit.
        _DEDUP_CHUNK = 1000
        for i in range(0, len(event_ids), _DEDUP_CHUNK):
            chunk = event_ids[i : i + _DEDUP_CHUNK]
            try:
                result = self._client.query(
                    f"SELECT event_id FROM {self.config.table}"
                    f" WHERE event_id IN {{event_ids:Array(String)}}",
                    parameters={"event_ids": chunk},
                )
                existing.update(row[0] for row in result.result_rows)
            except Exception:
                # If the query fails, let this chunk through.
                # The next pipeline run will catch any duplicates.
                pass

        return [ev for ev in events if ev.event_id not in existing]

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

    def close(self) -> None:
        self.flush()
        if self._client is not None:
            self._client.close()

    def dedup_aggregators(self) -> int:
        """Mark underlying DEX Swap events as 'swap_internal' when an
        aggregator-level swap exists in the same transaction.

        Per ENGINEERING_PLAN section 3.5: GROUP BY tx_hash — if an
        aggregator event (CoW Trade, 0x TransformedERC20, 1inch OrderFilled)
        exists in a tx, all other Swap events in that tx are reclassified
        as 'swap_internal' and excluded from trajectory queries by default.

        Returns the number of rows reclassified."""
        return _aggregator_dedup(self._ensure_client(), self.config.table)


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
        str(ev.amount_in) if ev.amount_in is not None else None,
        str(ev.amount_out) if ev.amount_out is not None else None,
        float(ev.amount_in_usd) if ev.amount_in_usd is not None else None,
        float(ev.amount_out_usd) if ev.amount_out_usd is not None else None,
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
        str(link.amount) if link.amount is not None else None,
        None,  # amount_usd (enriched later)
        1.0,  # link_confidence
        datetime.now(),
    ]


def _chunk_list(lst: list, size: int) -> list[list]:
    """Split a list into chunks of at most `size` elements."""
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def _aggregator_dedup(client, table: str) -> int:
    """Reclassify DEX Swap events as swap_internal when an aggregator
    event exists in the same transaction.

    Returns the count of rows that match the reclassification predicate
    BEFORE submitting the mutation. (CH `ALTER … UPDATE` is async, so
    `result.written_rows` isn't populated synchronously — this gives an
    accurate count for the log line at the cost of one extra SELECT.)
    """
    matched_q = client.query(
        f"SELECT count() FROM {table}"
        f" WHERE event_type = 'swap'"
        f" AND aggregator = ''"
        f" AND tx_hash IN ("
        f"  SELECT tx_hash FROM {table}"
        f"  WHERE aggregator != '' AND event_type = 'swap'"
        f" )"
    )
    matched = matched_q.result_rows[0][0] if matched_q.result_rows else 0

    if matched == 0:
        return 0

    try:
        client.command(
            f"ALTER TABLE {table}"
            f" UPDATE event_type = 'swap_internal'"
            f" WHERE tx_hash IN ("
            f"  SELECT tx_hash FROM {table}"
            f"  WHERE aggregator != '' AND event_type = 'swap'"
            f" )"
            f" AND event_type = 'swap'"
            f" AND aggregator = ''"
        )
        return matched
    except Exception:
        pass

    # Fallback: find affected rows via SELECT + batch UPDATE
    agg_txs = client.query(
        f"SELECT DISTINCT tx_hash FROM {table}"
        f" WHERE aggregator != '' AND event_type = 'swap'"
    )
    if not agg_txs.result_rows:
        return 0
    tx_hashes = [row[0] for row in agg_txs.result_rows]

    total = 0
    for tx_batch in _chunk_list(tx_hashes, 100):
        result = client.query(
            f"SELECT event_id FROM {table}"
            f" WHERE tx_hash IN {{txs:Array(String)}}"
            f" AND event_type = 'swap' AND aggregator = ''",
            parameters={"txs": tx_batch},
        )
        if not result.result_rows:
            continue
        event_ids = [row[0] for row in result.result_rows]
        for id_batch in _chunk_list(event_ids, 500):
            try:
                client.command(
                    f"ALTER TABLE {table}"
                    f" UPDATE event_type = 'swap_internal'"
                    f" WHERE event_id IN {{ids:Array(String)}}",
                    parameters={"ids": id_batch},
                )
            except Exception:
                pass
        total += len(event_ids)
    return total
