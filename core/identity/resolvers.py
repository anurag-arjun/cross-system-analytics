"""Identity resolvers: ENS, Farcaster, SmartWallet, etc."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx


@dataclass
class ResolutionResult:
    to_entity: str
    to_type: str
    relationship_type: str
    confidence: float
    source: str
    extra: dict[str, Any] | None = None


class Resolver(ABC):
    @abstractmethod
    def resolve(self, entity_id: str, entity_type: str) -> ResolutionResult | None: ...


class ENSResolver(Resolver):
    """Resolve wallet address to ENS name via public ENS gateway."""

    def __init__(self, endpoint: str = "https://api.ensdata.net") -> None:
        self.endpoint = endpoint
        self._client = httpx.Client(timeout=10.0)

    def resolve(self, entity_id: str, entity_type: str) -> ResolutionResult | None:
        if entity_type != "wallet":
            return None
        try:
            resp = self._client.get(f"{self.endpoint}/{entity_id}")
            if resp.status_code != 200:
                return None
            data = resp.json()
            ens = data.get("name")
            if not ens:
                return None
            return ResolutionResult(
                to_entity=ens,
                to_type="ens",
                relationship_type="resolved_to",
                confidence=0.95,
                source="ens",
                extra={"avatar": data.get("avatar")},
            )
        except Exception:
            return None

    def close(self) -> None:
        self._client.close()


class StaticResolver(Resolver):
    """Resolver for testing / hardcoded mappings."""

    def __init__(self, mappings: dict[str, ResolutionResult]) -> None:
        self.mappings = mappings

    def resolve(self, entity_id: str, entity_type: str) -> ResolutionResult | None:
        key = f"{entity_type}:{entity_id}"
        if key in self.mappings:
            return self.mappings[key]
        return None
