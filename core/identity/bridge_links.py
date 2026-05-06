"""Bridge link engine for cross-chain event matching."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from core.identity.pending_bridge import (
    InMemoryPendingBridgeStore,
    PendingBridgeStore,
)


@dataclass
class BridgeLink:
    link_key: str
    link_key_type: str
    src_chain: str
    src_block_time: datetime
    src_tx_hash: str
    src_entity_id: str
    src_event_id: str
    dst_chain: Optional[str] = None
    dst_block_time: Optional[datetime] = None
    dst_tx_hash: Optional[str] = None
    dst_entity_id: Optional[str] = None
    dst_event_id: Optional[str] = None
    token: Optional[str] = None
    amount: Optional[Any] = None


class BridgeLinkEngine:
    """Match bridge_out events with bridge_in events across chains.

    Uses a persistent PendingBridgeStore so cross-batch matching survives
    process restarts.  Matching is by (link_key, link_key_type) scoped to
    src_chain to prevent cross-chain depositId collisions (e.g. depositId=1
    on Base vs Ethereum are distinct).
    """

    def __init__(self, store: PendingBridgeStore | None = None) -> None:
        self._store = store or InMemoryPendingBridgeStore()
        self._links: list[BridgeLink] = []

    # ------------------------------------------------------------------
    # Persist bridge_out events
    # ------------------------------------------------------------------

    def add_bridge_out(self, event: dict[str, Any]) -> bool:
        """Register a bridge_out event in the pending store.

        Returns True if the row was newly inserted, False if it already
        existed (deduplicated by event_id).  Events without a link_key
        are silently skipped (they cannot be matched anyway).
        """
        link_key = event.get("link_key")
        if not link_key:
            return False
        return self._store.add_pending(event)

    # ------------------------------------------------------------------
    # Match bridge_in events against pending bridge_outs
    # ------------------------------------------------------------------

    def add_bridge_in(self, event: dict[str, Any]) -> Optional[BridgeLink]:
        """Try to match a bridge_in event with a pending bridge_out.

        Looks up candidates by (link_key, link_key_type) and filters to
        those whose src_chain differs from the bridge_in's chain (a valid
        bridge always moves between distinct chains).  If multiple
        candidates exist, the earliest src_block_time wins.

        Returns the completed BridgeLink if matched, None otherwise.
        """
        link_key = event.get("link_key")
        link_key_type = event.get("link_key_type")
        if not link_key or not link_key_type:
            return None

        dst_chain = event.get("chain", "")
        candidates = self._store.get_pending_by_link_key(link_key, link_key_type)

        # A bridge must go between *different* chains.
        candidates = [c for c in candidates if c.src_chain != dst_chain]
        if not candidates:
            return None

        # If multiple candidates (rare), pick the earliest.
        candidate = min(candidates, key=lambda c: c.src_block_time)

        matched = self._store.mark_matched(
            event_id=candidate.event_id,
            dst_chain=dst_chain,
            dst_block_time=event.get("timestamp", datetime.now(timezone.utc)),
            dst_tx_hash=event.get("tx_hash", ""),
            dst_entity_id=event.get("entity_id", ""),
            dst_event_id=event.get("event_id", ""),
        )
        if not matched:
            return None

        link = BridgeLink(
            link_key=link_key,
            link_key_type=link_key_type,
            src_chain=candidate.src_chain,
            src_block_time=candidate.src_block_time,
            src_tx_hash=candidate.src_tx_hash,
            src_entity_id=candidate.entity_id,
            src_event_id=candidate.event_id,
            dst_chain=dst_chain,
            dst_block_time=event.get("timestamp", datetime.now(timezone.utc)),
            dst_tx_hash=event.get("tx_hash", ""),
            dst_entity_id=event.get("entity_id", ""),
            dst_event_id=event.get("event_id", ""),
            token=candidate.token,
            amount=candidate.amount,
        )
        self._links.append(link)
        return link

    def get_pending(self) -> list[Any]:
        """Return bridge_out events awaiting bridge_in matches."""
        return self._store.get_pending_for_retry(datetime.max.replace(tzinfo=timezone.utc))

    def get_links(self) -> list[BridgeLink]:
        """Return all completed bridge links."""
        return self._links

    def match_batch(
        self,
        bridge_out_events: list[dict[str, Any]],
        bridge_in_events: list[dict[str, Any]],
    ) -> list[BridgeLink]:
        """Match a batch of bridge_out with bridge_in events."""
        for ev in bridge_out_events:
            self.add_bridge_out(ev)

        matched = []
        for ev in bridge_in_events:
            link = self.add_bridge_in(ev)
            if link:
                matched.append(link)

        return matched

    def stats(self) -> dict[str, int]:
        """Return matching statistics."""
        store_stats = self._store.get_stats()
        return {
            "pending": store_stats.get("pending", 0),
            "completed": len(self._links),
        }
