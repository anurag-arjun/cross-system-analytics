"""ABI-driven decoder.

A `GenericABIDecoder` is configured by a YAML mapping file that pairs an
event signature with field bindings to the canonical event schema. One
entry in the mapping produces one decoder instance.

Mapping YAML shape (see core/adapters/evm/decoders/mappings/README.md):

    protocol: uniswap_v3
    chains: [ethereum, base, arbitrum, optimism, polygon]
    events:
      - name: Swap
        inputs:
          - {name: sender,    type: address, indexed: true}
          - {name: recipient, type: address, indexed: true}
          - {name: amount0,   type: int256}
          - {name: amount1,   type: int256}
          - {name: sqrtPriceX96, type: uint160}
          - {name: liquidity, type: uint128}
          - {name: tick,      type: int24}
        canonical:
          event_type: swap
          entity_id:  "{sender}"
          venue:      "{log.address}"
          extra:
            recipient: "{recipient}"
        plugin: "core.adapters.evm.decoders.plugins:uniswap_v3_amounts"

Field bindings:
- A bare string is a literal ("swap").
- "{name}" looks up an event arg by name; "{log.address}" reads a log field.
- The optional `plugin` is a dotted "module:function" path. It receives
  (DecodedEvent, decoded_args, log) and returns the finalised event. Use it
  for protocol-specific logic that cannot be expressed declaratively
  (sign-based amount selection, pool-token resolution, multi-event
  dispatch by encoded selector).

Indexed args may appear at any position — Solidity does not require them
to be contiguous. UniV2's `Swap` is the canonical example: `sender` is
indexed at position 0 and `to` is indexed at position 5.
"""

from __future__ import annotations

import importlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import yaml
from eth_abi import decode as eth_abi_decode
from eth_utils import keccak

from core.adapters.evm.decoders.base import DecodedEvent, LogDecoder

PluginFn = Callable[[DecodedEvent, dict[str, Any], dict[str, Any]], DecodedEvent]


@dataclass(frozen=True)
class EventInput:
    name: str
    type: str
    indexed: bool = False


@dataclass(frozen=True)
class EventMapping:
    name: str
    inputs: tuple[EventInput, ...]
    canonical: dict[str, Any] = field(default_factory=dict)
    plugin: str | None = None

    @property
    def signature(self) -> str:
        return f"{self.name}({','.join(i.type for i in self.inputs)})"


@dataclass(frozen=True)
class ProtocolMapping:
    protocol: str
    chains: tuple[str, ...]
    events: tuple[EventMapping, ...]


_TEMPLATE_RE = re.compile(r"^\{([A-Za-z0-9_.]+)\}$")


def _topic0(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


def _import_plugin(spec: str) -> PluginFn:
    if ":" not in spec:
        raise ValueError(
            f"Plugin spec must be 'module.path:function_name', got {spec!r}"
        )
    module_path, fn_name = spec.split(":", 1)
    module = importlib.import_module(module_path)
    fn = getattr(module, fn_name, None)
    if fn is None:
        raise AttributeError(f"Plugin {spec!r}: function {fn_name} not found in {module_path}")
    return fn


def _decode_topic(topic: str, type_str: str) -> Any:
    """Decode a single 32-byte topic value as the given Solidity type."""
    raw = bytes.fromhex(topic[2:]) if topic.startswith("0x") else bytes.fromhex(topic)
    if len(raw) != 32:
        raise ValueError(f"Topic must be 32 bytes, got {len(raw)}")
    if type_str == "address":
        return "0x" + raw[-20:].hex()
    if type_str == "bool":
        return raw[-1] != 0
    if type_str.startswith("uint") or type_str.startswith("int"):
        return eth_abi_decode([type_str], raw)[0]
    if type_str == "bytes32":
        return "0x" + raw.hex()
    if type_str.startswith("bytes") and type_str != "bytes":
        n = int(type_str[5:])
        return "0x" + raw[:n].hex()
    # Dynamic types (string, bytes, arrays) are stored as keccak of value
    # when indexed — we surface the keccak as the value because the original
    # is lost.
    return "0x" + raw.hex()


class GenericABIDecoder(LogDecoder):
    """Decode a single event signature against a canonical-event mapping."""

    def __init__(self, mapping: ProtocolMapping, event: EventMapping) -> None:
        self._mapping = mapping
        self._event = event
        self._topic0 = _topic0(event.signature)
        self._indexed_positions = [i for i, inp in enumerate(event.inputs) if inp.indexed]
        self._non_indexed_positions = [
            i for i, inp in enumerate(event.inputs) if not inp.indexed
        ]
        self._plugin: PluginFn | None = (
            _import_plugin(event.plugin) if event.plugin else None
        )

    @property
    def topic0(self) -> str:
        return self._topic0

    @property
    def event_signature(self) -> str:
        return self._event.signature

    @property
    def protocol(self) -> str:
        return self._mapping.protocol

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 1 + len(self._indexed_positions):
            return None

        args: dict[str, Any] = {}

        for slot, pos in enumerate(self._indexed_positions, start=1):
            inp = self._event.inputs[pos]
            args[inp.name] = _decode_topic(topics[slot], inp.type)

        if self._non_indexed_positions:
            data = log.get("data", "0x")
            if data == "0x":
                return None
            non_indexed_types = [self._event.inputs[p].type for p in self._non_indexed_positions]
            decoded = eth_abi_decode(non_indexed_types, bytes.fromhex(data[2:]))
            for pos, val in zip(self._non_indexed_positions, decoded, strict=True):
                args[self._event.inputs[pos].name] = val

        ev = self._build_event(args, log, timestamp)
        if self._plugin is not None:
            ev = self._plugin(ev, args, log)
        return ev

    # ------------------------------------------------------------------
    # field binding helpers
    # ------------------------------------------------------------------

    def _resolve(self, expr: Any, args: dict[str, Any], log: dict[str, Any]) -> Any:
        """Resolve a field binding to its concrete value.

        - Non-string values pass through unchanged.
        - "{name}" -> args[name] or log fields if prefixed with "log.".
        - Plain strings are literals.
        """
        if not isinstance(expr, str):
            return expr
        m = _TEMPLATE_RE.match(expr)
        if not m:
            return expr
        path = m.group(1)
        if path.startswith("log."):
            field_name = path[4:]
            return log.get(field_name)
        if path in args:
            return args[path]
        raise KeyError(f"Template references unknown field {path!r}")

    def _resolve_str(self, expr: Any, args: dict[str, Any], log: dict[str, Any]) -> str:
        v = self._resolve(expr, args, log)
        return "" if v is None else str(v)

    def _resolve_optional_str(
        self, expr: Any, args: dict[str, Any], log: dict[str, Any]
    ) -> str | None:
        if expr is None:
            return None
        v = self._resolve(expr, args, log)
        return None if v is None else str(v)

    def _build_event(
        self, args: dict[str, Any], log: dict[str, Any], timestamp: datetime
    ) -> DecodedEvent:
        c = self._event.canonical

        extra = c.get("extra", {})
        rendered_extra = {
            k: self._resolve_str(v, args, log) for k, v in extra.items()
        }

        return DecodedEvent(
            event_type=self._resolve_str(c.get("event_type", ""), args, log),
            entity_id=self._resolve_str(c.get("entity_id", ""), args, log),
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self._mapping.protocol,
            venue=self._resolve_str(c.get("venue", ""), args, log),
            token_in=self._resolve_optional_str(c.get("token_in"), args, log),
            token_out=self._resolve_optional_str(c.get("token_out"), args, log),
            amount_in=self._resolve(c.get("amount_in"), args, log) if "amount_in" in c else None,
            amount_out=self._resolve(c.get("amount_out"), args, log) if "amount_out" in c else None,
            counterparty=self._resolve_optional_str(c.get("counterparty"), args, log),
            link_key=self._resolve_optional_str(c.get("link_key"), args, log),
            link_key_type=self._resolve_optional_str(c.get("link_key_type"), args, log),
            aggregator=self._resolve_str(c.get("aggregator", ""), args, log),
            extra=rendered_extra,
        )


# ----------------------------------------------------------------------
# YAML loading
# ----------------------------------------------------------------------


def load_mapping_file(path: Path) -> ProtocolMapping:
    raw = yaml.safe_load(path.read_text())
    return _build_protocol_mapping(raw, source=str(path))


def load_mapping_dir(path: Path) -> list[ProtocolMapping]:
    return [load_mapping_file(p) for p in sorted(path.glob("*.yaml"))]


def _build_event_mapping(raw: dict[str, Any]) -> EventMapping:
    inputs = tuple(
        EventInput(
            name=inp["name"],
            type=inp["type"],
            indexed=bool(inp.get("indexed", False)),
        )
        for inp in raw["inputs"]
    )
    return EventMapping(
        name=raw["name"],
        inputs=inputs,
        canonical=dict(raw.get("canonical", {})),
        plugin=raw.get("plugin"),
    )


def _build_protocol_mapping(raw: dict[str, Any], source: str) -> ProtocolMapping:
    try:
        events = tuple(_build_event_mapping(ev) for ev in raw["events"])
        return ProtocolMapping(
            protocol=raw["protocol"],
            chains=tuple(raw.get("chains", [])),
            events=events,
        )
    except (KeyError, TypeError) as e:
        raise ValueError(f"Invalid mapping at {source}: {e}") from e


def build_decoders(mapping: ProtocolMapping) -> list[GenericABIDecoder]:
    return [GenericABIDecoder(mapping, ev) for ev in mapping.events]
