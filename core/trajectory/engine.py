"""Trajectory query engine with identity graph resolution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import List, Optional

from core.adapters.base import CanonicalEvent
from core.identity.graph import IdentityGraph


@dataclass
class Filter:
    field: str
    op: str
    value: any


class TrajectoryEngine:
    """Query canonical events around an anchor event.

    Supports identity graph resolution: if entity_id is an ENS name,
    resolves to wallet first, then queries events.
    """

    def __init__(self, clickhouse_client, identity_graph: IdentityGraph | None = None) -> None:
        self.client = clickhouse_client
        self.graph = identity_graph

    def query(
        self,
        entity_id: str,
        anchor_event: str,
        window_before: timedelta,
        window_after: timedelta,
        entity_type: str = "wallet",
        filters: Optional[List[Filter]] = None,
        include_anchor: bool = True,
    ) -> List[CanonicalEvent]:
        """Return events for entity_id within time window around anchor_event.

        If entity_type is not 'wallet' and an identity graph is configured,
        attempts to resolve to wallet first.
        """
        resolved_id = self._resolve_to_wallet(entity_id, entity_type)
        if resolved_id is None:
            return []

        # TODO: ClickHouse query via ORDER BY (entity_id, timestamp)
        # For now, return empty (engine is stub until ClickHouse has data)
        return []

    def query_cross_chain(
        self,
        entity_id: str,
        anchor_event: str,
        window_before: timedelta,
        window_after: timedelta,
        entity_type: str = "wallet",
    ) -> List[CanonicalEvent]:
        """Cross-chain trajectory: resolve bridge links, include dst_chain events."""
        resolved_id = self._resolve_to_wallet(entity_id, entity_type)
        if resolved_id is None:
            return []
        # TODO: Bridge link resolution + cross-chain query
        return []

    def _resolve_to_wallet(self, entity_id: str, entity_type: str) -> str | None:
        """If entity is not a wallet, try identity graph to find one."""
        if entity_type == "wallet":
            return entity_id
        if self.graph is None:
            return None
        results = self.graph.resolve(
            entity_id, entity_type=entity_type, target_type="wallet", max_depth=3
        )
        if results:
            return results[0].entity_id
        return None
