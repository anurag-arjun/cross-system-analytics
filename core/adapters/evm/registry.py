"""Protocol registry: maps (topic0, address) -> decoder.

Pattern from archive/conv.md and ENGINEERING_PLAN.md:
  build a registry of (protocol, version, chain, topic0, address_pattern, decoder)

Address patterns:
  - Exact address: "0x4200...0006"
  - Wildcard: "*" matches any address (factory-derived pools)
  - Prefix: "0x3e47*" for known contract families
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.adapters.evm.decoders import DEFAULT_DECODERS, LogDecoder


@dataclass(frozen=True)
class ProtocolEntry:
    protocol: str
    version: str
    chain: str
    topic0: str
    address_pattern: str
    decoder: LogDecoder
    event_name: str = ""
    event_signature: str = ""


class DecoderRegistry:
    """Lookup decoders by topic0 + optional address filter."""

    def __init__(self, entries: list[ProtocolEntry] | None = None) -> None:
        self._by_topic0: dict[str, list[ProtocolEntry]] = {}
        for entry in entries or []:
            self._by_topic0.setdefault(entry.topic0, []).append(entry)

    def register(self, entry: ProtocolEntry) -> None:
        self._by_topic0.setdefault(entry.topic0, []).append(entry)

    def lookup(self, topic0: str, address: str | None = None) -> LogDecoder | None:
        entries = self._by_topic0.get(topic0, [])
        if not entries:
            return None
        if address is None or len(entries) == 1:
            return entries[0].decoder
        for entry in entries:
            if self._match_address(entry.address_pattern, address):
                return entry.decoder
        return None

    def _match_address(self, pattern: str, address: str) -> bool:
        if pattern == "*":
            return True
        if pattern.lower() == address.lower():
            return True
        if pattern.endswith("*") and address.lower().startswith(pattern[:-1].lower()):
            return True
        return False

    def all_topic0s(self) -> list[str]:
        return list(self._by_topic0.keys())

    def all_decoders(self) -> list[LogDecoder]:
        seen: set[str] = set()
        result: list[LogDecoder] = []
        for entries in self._by_topic0.values():
            for entry in entries:
                key = f"{entry.topic0}:{entry.address_pattern}"
                if key not in seen:
                    seen.add(key)
                    result.append(entry.decoder)
        return result


def build_default_registry() -> DecoderRegistry:
    """Build registry with all built-in decoders."""
    registry = DecoderRegistry()
    for decoder in DEFAULT_DECODERS:
        registry.register(
            ProtocolEntry(
                protocol=decoder.protocol,
                version="1",
                chain="*",
                topic0=decoder.topic0,
                address_pattern="*",
                decoder=decoder,
                event_name=decoder.__class__.__name__.replace("Decoder", ""),
                event_signature=decoder.event_signature,
            )
        )
    return registry
