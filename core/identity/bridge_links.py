"""Bridge link engine for cross-chain event matching."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Optional

from core.identity.chain_mapping import normalize_chain
from core.identity.pending_bridge import (
    InMemoryPendingBridgeStore,
    PendingBridgeOut,
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

        For Stargate, link_keys use different ID systems on each side
        (chain_id vs endpoint_id).  We normalize both to chain names
        and match heuristically with amount/time tolerances.

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

        if not candidates and link_key_type in ("stargate_dst_chain", "stargate_src_eid", "layerzero_src_eid"):
            candidates = self._match_stargate_chain_normalized(event, dst_chain)

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

    # ------------------------------------------------------------------
    # Stargate: chain-ID-normalized matching
    # ------------------------------------------------------------------

    _STARGATE_KEY_TYPES = ("stargate_dst_chain", "stargate_src_eid", "layerzero_src_eid")
    _STARGATE_AMOUNT_TOLERANCE = Decimal("0.005")  # 0.5%
    _STARGATE_TIME_WINDOW = timedelta(minutes=30)

    def _match_stargate_chain_normalized(
        self, event: dict[str, Any], dst_chain: str
    ) -> list[PendingBridgeOut]:
        """Match Stargate bridge_in by normalizing chain IDs to chain names.

        Cross-referencing logic:
          bridge_out: src_chain=A, dst_chain_id=X → dest_chain_name
          bridge_in:  chain=B, src_eid=Y → source_chain_name
          Match when: bridge_out.dest_chain == bridge_in.chain
                  AND bridge_out.src_chain == bridge_in.source_chain
        """
        link_key = event.get("link_key")
        link_key_type = event.get("link_key_type")

        # Normalize the bridge_in's src_eid → source chain name
        in_source_chain = normalize_chain(link_key, link_key_type)
        if not in_source_chain:
            return []

        # The bridge_in arrived on `dst_chain` — this must match the
        # bridge_out's destination chain (normalized from dst_chain_id).
        in_dest_chain = dst_chain

        # Fetch ALL pending Stargate/LZ bridge_outs.
        all_pending = self._store.get_pending_for_retry(
            datetime.max.replace(tzinfo=timezone.utc)
        )

        candidates: list[PendingBridgeOut] = []
        bridge_in_ts = event.get("timestamp")

        for p in all_pending:
            if p.link_key_type not in self._STARGATE_KEY_TYPES:
                continue
            if p.src_chain == dst_chain:
                continue  # must be cross-chain

            # Bridge_out: src_chain=A, dst_chain_id=normalize→dest_name
            p_dest_chain = normalize_chain(p.link_key, p.link_key_type)
            if p_dest_chain != in_dest_chain:
                continue  # bridge_in's chain must match bridge_out's destination

            # Bridge_out's source chain must match bridge_in's normalized source
            if p.src_chain != in_source_chain:
                continue

            # Heuristic: similar amount.
            p_amount = p.amount
            ev_amt = event.get("amount")
            if p_amount is not None and ev_amt is not None:
                try:
                    p_dec = Decimal(str(p_amount))
                    ev_dec = Decimal(str(ev_amt))
                    if p_dec > 0 and ev_dec > 0:
                        ratio = abs(p_dec - ev_dec) / max(p_dec, ev_dec)
                        if ratio > self._STARGATE_AMOUNT_TOLERANCE:
                            continue
                except (ValueError, TypeError, Decimal.InvalidOperation):
                    pass

            # Heuristic: time window.
            if bridge_in_ts is not None and p.src_block_time is not None:
                delta = abs(bridge_in_ts - p.src_block_time)
                if delta > self._STARGATE_TIME_WINDOW:
                    continue

            candidates.append(p)

        return candidates

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
