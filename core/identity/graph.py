"""Identity graph for cross-system entity resolution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional


@dataclass(frozen=True)
class ResolvedEntity:
    entity_id: str
    entity_type: str
    confidence: float
    source: str
    detected_at: datetime
    path: tuple[str, ...] = ()


class IdentityGraph:
    """In-memory identity graph with multi-hop resolution.

    Stores relationships as directed edges with confidence scores.
    Supports BFS traversal with confidence propagation.
    """

    def __init__(self) -> None:
        # Adjacency list: entity_id -> list of (to_entity, to_type, relationship, confidence, source, detected_at)
        self._edges: dict[str, list[tuple[str, str, str, float, str, datetime]]] = {}

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_relationship(
        self,
        from_entity: str,
        from_type: str,
        to_entity: str,
        to_type: str,
        relationship_type: str,
        confidence: float,
        source: str,
        expires_at: Optional[datetime] = None,
    ) -> None:
        key = _entity_key(from_entity, from_type)
        self._edges.setdefault(key, []).append(
            (to_entity, to_type, relationship_type, confidence, source, datetime.now(timezone.utc))
        )

    def bulk_load(self, rows: list[dict[str, Any]]) -> None:
        """Load relationships from ClickHouse result format."""
        for row in rows:
            self.add_relationship(
                from_entity=row["from_entity"],
                from_type=row["from_entity_type"],
                to_entity=row["to_entity"],
                to_type=row["to_entity_type"],
                relationship_type=row["relationship_type"],
                confidence=row["confidence_score"],
                source=row["source"],
            )

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def resolve(
        self,
        entity_id: str,
        entity_type: str = "wallet",
        target_type: Optional[str] = None,
        min_confidence: float = 0.5,
        max_depth: int = 3,
    ) -> list[ResolvedEntity]:
        """BFS traversal from entity_id, collecting all reachable entities.

        Confidence is propagated multiplicatively across hops:
          wallet --0.9--> ens --0.8--> twitter  =>  0.72
        """
        start_key = _entity_key(entity_id, entity_type)
        if start_key not in self._edges and not any(
            start_key == e[0] for edges in self._edges.values() for e in edges
        ):
            return []

        results: list[ResolvedEntity] = []
        visited: set[str] = set()
        queue: list[tuple[str, float, tuple[str, ...]]] = [(start_key, 1.0, (start_key,))]

        while queue:
            current_key, current_confidence, path = queue.pop(0)

            if current_key in visited:
                continue
            visited.add(current_key)

            # If we've moved at least one hop and match target type (or no target filter)
            if len(path) > 1:
                current_id, current_type = _split_key(current_key)
                if target_type is None or current_type == target_type:
                    if current_confidence >= min_confidence:
                        results.append(
                            ResolvedEntity(
                                entity_id=current_id,
                                entity_type=current_type,
                                confidence=round(current_confidence, 4),
                                source="graph_walk",
                                detected_at=datetime.now(timezone.utc),
                                path=path,
                            )
                        )

            if len(path) >= max_depth + 1:
                continue

            for to_entity, to_type, _rel, edge_conf, source, _dt in self._edges.get(
                current_key, []
            ):
                next_key = _entity_key(to_entity, to_type)
                next_confidence = current_confidence * edge_conf
                if next_confidence >= min_confidence:
                    queue.append((next_key, next_confidence, path + (next_key,)))

        # Sort by confidence descending
        results.sort(key=lambda r: r.confidence, reverse=True)
        return results

    def walk(
        self,
        entity_id: str,
        entity_type: str,
        path_types: list[str],
    ) -> list[ResolvedEntity]:
        """Walk a specific path of entity types, e.g. ['wallet', 'ens', 'twitter'].

        Returns entities at the end of the path that satisfy the full chain.
        """
        if not path_types:
            return []

        # Start from the first type in the path
        start_key = _entity_key(entity_id, entity_type)
        if start_key not in self._edges:
            return []

        current_results: list[tuple[str, float, tuple[str, ...]]] = [(start_key, 1.0, (start_key,))]

        for next_type in path_types[1:]:
            next_results: list[tuple[str, float, tuple[str, ...]]] = []
            for current_key, current_conf, path in current_results:
                for to_entity, to_type, _rel, edge_conf, _source, _dt in self._edges.get(
                    current_key, []
                ):
                    if to_type == next_type:
                        next_key = _entity_key(to_entity, to_type)
                        next_results.append(
                            (next_key, current_conf * edge_conf, path + (next_key,))
                        )
            current_results = next_results
            if not current_results:
                return []

        return [
            ResolvedEntity(
                entity_id=_split_key(key)[0],
                entity_type=_split_key(key)[1],
                confidence=round(conf, 4),
                source="path_walk",
                detected_at=datetime.now(timezone.utc),
                path=path,
            )
            for key, conf, path in current_results
        ]

    # ------------------------------------------------------------------
    # Stats / debug
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, int]:
        """Return graph statistics."""
        total_edges = sum(len(e) for e in self._edges.values())
        return {
            "entities": len(self._edges),
            "relationships": total_edges,
        }


def _entity_key(entity_id: str, entity_type: str) -> str:
    return f"{entity_type}:{entity_id}"


def _split_key(key: str) -> tuple[str, str]:
    parts = key.split(":", 1)
    return parts[1], parts[0]
