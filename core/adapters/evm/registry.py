"""Decoder registry: resolves a log to the right decoder.

Two lookup paths, tried in order:

1. **Address-first** — when a `protocol_resolver` is configured, the registry
   asks "what protocol does this (chain, address) belong to?" If the resolver
   returns a known protocol and that protocol has a decoder for this topic0,
   that decoder wins. This path is what makes shared-topic events
   (e.g. ERC20 Transfer, UniV3-fork Swap) attributable to the correct
   protocol — the registry uses contract identity, not just the event hash.

2. **Topic0** — fallback for bespoke decoders that own a globally-unique
   topic0 (Across `FundsDeposited`, OP `MessagePassed`, 1inch `OrderFilled`,
   etc.). The first matching entry wins; entries with a specific
   `address_pattern` are preferred over wildcards.

The protocol resolver is supplied by the EVM adapter at startup. Today it
can be an in-memory dict; once `na-9tmq` (protocol_contracts table) lands,
it becomes a Postgres-backed lookup.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from core.adapters.evm.decoders import DEFAULT_DECODERS, LogDecoder
from core.adapters.evm.decoders.generic import (
    GenericABIDecoder,
    build_decoders,
    load_mapping_dir,
)

ProtocolResolver = Callable[[str, str], str | None]
"""Given (chain, contract_address) -> protocol slug or None."""


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
    """Lookup decoders by (chain, address) first, falling back to topic0."""

    def __init__(
        self,
        entries: list[ProtocolEntry] | None = None,
        protocol_resolver: ProtocolResolver | None = None,
    ) -> None:
        self._by_topic0: dict[str, list[ProtocolEntry]] = {}
        self._by_protocol: dict[str, dict[str, ProtocolEntry]] = {}
        self._protocol_resolver = protocol_resolver
        for entry in entries or []:
            self.register(entry)

    def register(self, entry: ProtocolEntry) -> None:
        self._by_topic0.setdefault(entry.topic0, []).append(entry)
        # Sort topic0 entries: specific addresses ahead of wildcards.
        self._by_topic0[entry.topic0].sort(key=lambda e: e.address_pattern == "*")
        self._by_protocol.setdefault(entry.protocol, {})[entry.topic0] = entry

    def set_protocol_resolver(self, resolver: ProtocolResolver | None) -> None:
        self._protocol_resolver = resolver

    def lookup(
        self,
        topic0: str,
        address: str | None = None,
        chain: str | None = None,
    ) -> LogDecoder | None:
        if chain and address and self._protocol_resolver is not None:
            protocol = self._protocol_resolver(chain, address.lower())
            if protocol:
                entry = self._by_protocol.get(protocol, {}).get(topic0)
                if entry is not None:
                    return entry.decoder

        entries = self._by_topic0.get(topic0, [])
        if not entries:
            return None
        if address is None:
            return entries[0].decoder
        for entry in entries:
            if self._match_address(entry.address_pattern, address):
                return entry.decoder
        return None

    @staticmethod
    def _match_address(pattern: str, address: str) -> bool:
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


# ----------------------------------------------------------------------
# Default registry: bespoke classes + YAML-driven mappings
# ----------------------------------------------------------------------

_MAPPINGS_DIR = Path(__file__).parent / "decoders" / "mappings"


def _entry_from_decoder(decoder: LogDecoder) -> ProtocolEntry:
    return ProtocolEntry(
        protocol=decoder.protocol,
        version="1",
        chain="*",
        topic0=decoder.topic0,
        address_pattern="*",
        decoder=decoder,
        event_name=decoder.__class__.__name__.replace("Decoder", ""),
        event_signature=decoder.event_signature,
    )


def _load_mapping_decoders() -> list[GenericABIDecoder]:
    """Load YAML mappings with hand-curated parents registered first.

    Many DEX-fork mappings share a topic0 (every UniV2 fork emits the same
    `Swap` signature). Address-first lookup resolves the correct protocol
    when the contract address is in `protocol_contracts`; for unknown
    addresses the registry falls back to the first decoder registered for
    that topic0. Parents (hand-curated, no `template:` field) should win
    that fallback, so we register them ahead of template-aliased children.
    """
    if not _MAPPINGS_DIR.exists():
        return []

    import yaml as _yaml

    mappings_by_protocol = {m.protocol: m for m in load_mapping_dir(_MAPPINGS_DIR)}

    parents: list[GenericABIDecoder] = []
    children: list[GenericABIDecoder] = []
    for path in sorted(_MAPPINGS_DIR.glob("*.yaml")):
        raw = _yaml.safe_load(path.read_text())
        if not isinstance(raw, dict):
            continue
        mapping = mappings_by_protocol.get(raw.get("protocol"))
        if mapping is None:
            continue
        bucket = children if "template" in raw else parents
        bucket.extend(build_decoders(mapping))
    return parents + children


def build_default_registry(
    protocol_resolver: ProtocolResolver | None = None,
) -> DecoderRegistry:
    """Build a registry with all built-in bespoke decoders + YAML mappings."""
    registry = DecoderRegistry(protocol_resolver=protocol_resolver)

    for decoder in DEFAULT_DECODERS:
        registry.register(_entry_from_decoder(decoder))

    for decoder in _load_mapping_decoders():
        registry.register(_entry_from_decoder(decoder))

    return registry
