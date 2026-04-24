from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterator

from core.identity.graph import IdentityGraph
from core.identity.resolvers import ENSResolver, ResolutionResult, Resolver


class ResolutionPipeline:
    def __init__(
        self,
        resolvers: list[Resolver] | None = None,
        identity_graph: IdentityGraph | None = None,
    ) -> None:
        self.resolvers = resolvers or [ENSResolver()]
        self.graph = identity_graph or IdentityGraph()
        self._results: list[dict[str, Any]] = []

    def resolve_batch(
        self,
        entities: list[tuple[str, str]],
    ) -> list[dict[str, Any]]:
        resolved: list[dict[str, Any]] = []
        for entity_id, entity_type in entities:
            for resolver in self.resolvers:
                result = resolver.resolve(entity_id, entity_type)
                if result is not None:
                    self.graph.add_relationship(
                        from_entity=entity_id,
                        from_type=entity_type,
                        to_entity=result.to_entity,
                        to_type=result.to_type,
                        relationship_type=result.relationship_type,
                        confidence=result.confidence,
                        source=result.source,
                    )
                    row = {
                        "from_entity": entity_id,
                        "from_entity_type": entity_type,
                        "to_entity": result.to_entity,
                        "to_entity_type": result.to_type,
                        "relationship_type": result.relationship_type,
                        "confidence_score": result.confidence,
                        "source": result.source,
                        "detected_at": datetime.now(timezone.utc),
                        "extra": result.extra,
                    }
                    resolved.append(row)
                    self._results.append(row)
                    break
        return resolved

    def resolve_wallets(
        self,
        wallet_addresses: list[str],
    ) -> list[dict[str, Any]]:
        entities = [(addr, "wallet") for addr in wallet_addresses]
        return self.resolve_batch(entities)

    def to_clickhouse_rows(self) -> list[list[Any]]:
        return [
            [
                r["from_entity"],
                r["from_entity_type"],
                r["to_entity"],
                r["to_entity_type"],
                r["relationship_type"],
                r["confidence_score"],
                r["source"],
                r["detected_at"],
                None,
            ]
            for r in self._results
        ]

    def close(self) -> None:
        for resolver in self.resolvers:
            if hasattr(resolver, "close"):
                resolver.close()
