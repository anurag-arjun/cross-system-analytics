"""Bridge link engine for cross-chain event matching."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional


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

    Supports multiple bridge protocols via link_key matching:
    - Across: deposit_id
    - Stargate: dst_chain_id (approximate, may need additional heuristics)
    - Base native: tx_hash + suffix
    """

    def __init__(self) -> None:
        # Pending bridge_out events waiting for bridge_in matches
        self._pending_out: dict[str, BridgeLink] = {}
        # Completed links
        self._links: list[BridgeLink] = []

    def add_bridge_out(self, event: dict[str, Any]) -> None:
        """Register a bridge_out event from canonical_events row."""
        link_key = event.get("link_key")
        if not link_key:
            return

        link = BridgeLink(
            link_key=link_key,
            link_key_type=event.get("link_key_type", "unknown"),
            src_chain=event.get("chain", ""),
            src_block_time=event.get("timestamp", datetime.now(timezone.utc)),
            src_tx_hash=event.get("tx_hash", ""),
            src_entity_id=event.get("entity_id", ""),
            src_event_id=event.get("event_id", ""),
            token=event.get("token_out"),
            amount=event.get("amount_out"),
        )
        self._pending_out[link_key] = link

    def add_bridge_in(self, event: dict[str, Any]) -> Optional[BridgeLink]:
        """Try to match a bridge_in event with a pending bridge_out.

        Returns the completed BridgeLink if matched, None otherwise.
        """
        link_key = event.get("link_key")
        if not link_key or link_key not in self._pending_out:
            return None

        link = self._pending_out.pop(link_key)
        link.dst_chain = event.get("chain", "")
        link.dst_block_time = event.get("timestamp", datetime.now(timezone.utc))
        link.dst_tx_hash = event.get("tx_hash", "")
        link.dst_entity_id = event.get("entity_id", "")
        link.dst_event_id = event.get("event_id", "")

        self._links.append(link)
        return link

    def get_pending(self) -> list[BridgeLink]:
        """Return bridge_out events awaiting bridge_in matches."""
        return list(self._pending_out.values())

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
        return {
            "pending": len(self._pending_out),
            "completed": len(self._links),
        }
