"""Persistent store for pending bridge_out events with retry scheduling."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

# Retry cadences by bridge family (link_key_type).
# These reflect typical bridge latencies:
#   - Across: solvers fill in minutes → retry every 2 min
#   - Canonical bridges (OP Stack, Arbitrum): 7-day challenge period → retry every 6 hr
RETRY_CADENCES: dict[str, timedelta] = {
    "across_deposit_id": timedelta(minutes=2),
    "op_stack_bridge": timedelta(hours=6),
    "op_withdrawal_hash": timedelta(hours=6),
    "stargate_dst_chain": timedelta(minutes=10),
    "stargate_src_eid": timedelta(minutes=10),
    "op_withdrawal_hash": timedelta(hours=6),
    "arbitrum_message_num": timedelta(hours=6),
}
DEFAULT_RETRY_CADENCE = timedelta(minutes=10)
DEFAULT_EXPIRY_DAYS = 30


@dataclass
class PendingBridgeOut:
    """A bridge_out event awaiting its bridge_in counterpart."""

    event_id: str
    entity_id: str
    entity_type: str
    link_key: str
    link_key_type: str
    src_chain: str
    src_block_time: datetime
    src_tx_hash: str
    src_event_id: str
    token: Optional[str] = None
    amount: Optional[Any] = None
    status: str = "pending"
    retry_count: int = 0
    next_retry_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    matched_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None

    @classmethod
    def from_canonical_event(cls, event: dict[str, Any]) -> "PendingBridgeOut":
        """Build a PendingBridgeOut from a decoded canonical event dict."""
        now = datetime.now(timezone.utc)
        link_key_type = event.get("link_key_type", "unknown")
        cadence = RETRY_CADENCES.get(link_key_type, DEFAULT_RETRY_CADENCE)
        return cls(
            event_id=event["event_id"],
            entity_id=event["entity_id"],
            entity_type=event.get("entity_type", "wallet"),
            link_key=event["link_key"],
            link_key_type=link_key_type,
            src_chain=event.get("chain", ""),
            src_block_time=event.get("timestamp", now),
            src_tx_hash=event.get("tx_hash", ""),
            src_event_id=event["event_id"],
            token=event.get("token_out"),
            amount=event.get("amount_out"),
            status="pending",
            retry_count=0,
            next_retry_at=now + cadence,
            created_at=now,
            expires_at=now + timedelta(days=DEFAULT_EXPIRY_DAYS),
        )


class PendingBridgeStore(ABC):
    """Abstract store for pending bridge_out events."""

    @abstractmethod
    def add_pending(self, event: dict[str, Any]) -> bool:
        """Persist a bridge_out event as pending. Returns True if inserted, False if duplicate."""
        ...

    @abstractmethod
    def get_pending_by_link_key(
        self,
        link_key: str,
        link_key_type: str,
    ) -> list[PendingBridgeOut]:
        """Return all pending bridge_outs with the given link_key + link_key_type."""
        ...

    @abstractmethod
    def get_pending_for_retry(self, before: datetime) -> list[PendingBridgeOut]:
        """Return pending rows where next_retry_at <= before."""
        ...

    @abstractmethod
    def mark_matched(
        self,
        event_id: str,
        dst_chain: str,
        dst_block_time: datetime,
        dst_tx_hash: str,
        dst_entity_id: str,
        dst_event_id: str,
    ) -> bool:
        """Mark a pending bridge_out as matched. Returns True if row existed and was updated."""
        ...

    @abstractmethod
    def delete_expired(self, before: datetime) -> int:
        """Delete expired pending rows. Returns count deleted."""
        ...

    @abstractmethod
    def update_retry_schedule(self, event_id: str) -> bool:
        """Bump retry_count and set next_retry_at based on link_key_type cadence."""
        ...

    @abstractmethod
    def get_stats(self) -> dict[str, int]:
        """Return counts by status."""
        ...


class InMemoryPendingBridgeStore(PendingBridgeStore):
    """In-memory store for testing. Not thread-safe."""

    def __init__(self) -> None:
        self._rows: dict[str, PendingBridgeOut] = {}

    def add_pending(self, event: dict[str, Any]) -> bool:
        row = PendingBridgeOut.from_canonical_event(event)
        if row.event_id in self._rows:
            return False
        self._rows[row.event_id] = row
        return True

    def get_pending_by_link_key(self, link_key: str, link_key_type: str) -> list[PendingBridgeOut]:
        return [
            r
            for r in self._rows.values()
            if r.link_key == link_key and r.link_key_type == link_key_type and r.status == "pending"
        ]

    def get_pending_for_retry(self, before: datetime) -> list[PendingBridgeOut]:
        return [
            r
            for r in self._rows.values()
            if r.status == "pending" and r.next_retry_at is not None and r.next_retry_at <= before
        ]

    def mark_matched(
        self,
        event_id: str,
        dst_chain: str,
        dst_block_time: datetime,
        dst_tx_hash: str,
        dst_entity_id: str,
        dst_event_id: str,
    ) -> bool:
        row = self._rows.get(event_id)
        if row is None or row.status != "pending":
            return False
        row.status = "matched"
        row.matched_at = datetime.now(timezone.utc)
        # Store dst info in extra-like fields via a side dict (not in dataclass for simplicity)
        row.__dict__["_dst_chain"] = dst_chain
        row.__dict__["_dst_tx_hash"] = dst_tx_hash
        row.__dict__["_dst_event_id"] = dst_event_id
        return True

    def delete_expired(self, before: datetime) -> int:
        expired = [k for k, r in self._rows.items() if r.status == "pending" and r.expires_at is not None and r.expires_at <= before]
        for k in expired:
            del self._rows[k]
        return len(expired)

    def update_retry_schedule(self, event_id: str) -> bool:
        row = self._rows.get(event_id)
        if row is None or row.status != "pending":
            return False
        row.retry_count += 1
        cadence = RETRY_CADENCES.get(row.link_key_type, DEFAULT_RETRY_CADENCE)
        row.next_retry_at = datetime.now(timezone.utc) + cadence
        return True

    def get_stats(self) -> dict[str, int]:
        stats: dict[str, int] = {}
        for r in self._rows.values():
            stats[r.status] = stats.get(r.status, 0) + 1
        return stats


class PostgresPendingBridgeStore(PendingBridgeStore):
    """Postgres-backed store for pending bridge_out events.

    Expects a table created by /core/schemas/postgres/pending_bridge_outs.sql.
    """

    def __init__(self, dsn: str) -> None:
        import psycopg2

        self._dsn = dsn
        self._connect = lambda: psycopg2.connect(dsn)

    def _conn(self):
        return self._connect()

    def add_pending(self, event: dict[str, Any]) -> bool:
        import psycopg2.errors

        row = PendingBridgeOut.from_canonical_event(event)
        sql = """
            INSERT INTO pending_bridge_outs (
                event_id, entity_id, entity_type,
                link_key, link_key_type, src_chain,
                src_block_time, src_tx_hash, src_event_id,
                token, amount,
                status, retry_count, next_retry_at,
                created_at, expires_at
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s
            )
            ON CONFLICT (event_id) DO NOTHING
        """
        try:
            with self._conn() as conn, conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        row.event_id,
                        row.entity_id,
                        row.entity_type,
                        row.link_key,
                        row.link_key_type,
                        row.src_chain,
                        row.src_block_time,
                        row.src_tx_hash,
                        row.src_event_id,
                        row.token,
                        str(row.amount) if row.amount is not None else None,
                        row.status,
                        row.retry_count,
                        row.next_retry_at,
                        row.created_at,
                        row.expires_at,
                    ),
                )
                conn.commit()
                return cur.rowcount > 0
        except psycopg2.errors.UniqueViolation:
            return False

    def get_pending_by_link_key(self, link_key: str, link_key_type: str) -> list[PendingBridgeOut]:
        sql = """
            SELECT
                event_id, entity_id, entity_type,
                link_key, link_key_type, src_chain,
                src_block_time, src_tx_hash, src_event_id,
                token, amount,
                status, retry_count, next_retry_at,
                created_at, matched_at, expires_at
            FROM pending_bridge_outs
            WHERE link_key = %s
              AND link_key_type = %s
              AND status = 'pending'
            ORDER BY src_block_time ASC
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (link_key, link_key_type))
            return [self._row_to_pending(cur.description, row) for row in cur.fetchall()]

    def get_pending_for_retry(self, before: datetime) -> list[PendingBridgeOut]:
        sql = """
            SELECT
                event_id, entity_id, entity_type,
                link_key, link_key_type, src_chain,
                src_block_time, src_tx_hash, src_event_id,
                token, amount,
                status, retry_count, next_retry_at,
                created_at, matched_at, expires_at
            FROM pending_bridge_outs
            WHERE status = 'pending'
              AND next_retry_at <= %s
            ORDER BY next_retry_at ASC
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (before,))
            return [self._row_to_pending(cur.description, row) for row in cur.fetchall()]

    def mark_matched(
        self,
        event_id: str,
        dst_chain: str,
        dst_block_time: datetime,
        dst_tx_hash: str,
        dst_entity_id: str,
        dst_event_id: str,
    ) -> bool:
        sql = """
            UPDATE pending_bridge_outs
            SET status = 'matched',
                matched_at = NOW()
            WHERE event_id = %s
              AND status = 'pending'
        """
        # dst_* are not stored in pending_bridge_outs currently;
        # they belong in bridge_links.  We just mark the pending row.
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (event_id,))
            conn.commit()
            return cur.rowcount > 0

    def delete_expired(self, before: datetime) -> int:
        sql = """
            DELETE FROM pending_bridge_outs
            WHERE status = 'pending'
              AND expires_at <= %s
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (before,))
            conn.commit()
            return cur.rowcount

    def update_retry_schedule(self, event_id: str) -> bool:
        sql = """
            UPDATE pending_bridge_outs
            SET retry_count = retry_count + 1,
                next_retry_at = NOW() + (
                    CASE link_key_type
                        WHEN 'across_deposit_id' THEN INTERVAL '2 minutes'
                        WHEN 'op_stack_bridge' THEN INTERVAL '6 hours'
                        WHEN 'op_withdrawal_hash' THEN INTERVAL '6 hours'
                        WHEN 'stargate_dst_chain' THEN INTERVAL '10 minutes'
                        WHEN 'stargate_src_eid' THEN INTERVAL '10 minutes'
                        WHEN 'op_withdrawal_hash' THEN INTERVAL '6 hours'
                        WHEN 'arbitrum_message_num' THEN INTERVAL '6 hours'
                        ELSE INTERVAL '10 minutes'
                    END
                )
            WHERE event_id = %s
              AND status = 'pending'
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (event_id,))
            conn.commit()
            return cur.rowcount > 0

    def get_stats(self) -> dict[str, int]:
        sql = """
            SELECT status, COUNT(*)
            FROM pending_bridge_outs
            GROUP BY status
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            return {row[0]: row[1] for row in cur.fetchall()}

    @staticmethod
    def _row_to_pending(description, row) -> PendingBridgeOut:
        names = [d.name for d in description]
        d = dict(zip(names, row))
        return PendingBridgeOut(
            event_id=d["event_id"],
            entity_id=d["entity_id"],
            entity_type=d["entity_type"],
            link_key=d["link_key"],
            link_key_type=d["link_key_type"],
            src_chain=d["src_chain"],
            src_block_time=d["src_block_time"],
            src_tx_hash=d["src_tx_hash"],
            src_event_id=d["src_event_id"],
            token=d.get("token"),
            amount=d.get("amount"),
            status=d["status"],
            retry_count=d["retry_count"],
            next_retry_at=d.get("next_retry_at"),
            created_at=d.get("created_at"),
            matched_at=d.get("matched_at"),
            expires_at=d.get("expires_at"),
        )
