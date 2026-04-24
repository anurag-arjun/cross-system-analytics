"""Base decoder interface for EVM log events.

Decoders are pure functions of (topic0, data, topics) -> DecodedEvent.
They are stateless and per-event. Do not write one big "swap normalizer".
Instead write decode_uniswap_v3_swap(log) -> CanonicalSwap, etc., and compose.

Reference: Dune's dex.trades spellbook for event signatures across protocols.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any

from eth_abi import decode as eth_abi_decode


@dataclass
class DecodedEvent:
    event_type: str
    entity_id: str
    timestamp: datetime
    block_number: int
    tx_hash: str
    log_index: int
    protocol: str = ""
    venue: str = ""
    token_in: str | None = None
    token_out: str | None = None
    amount_in: Decimal | None = None
    amount_out: Decimal | None = None
    counterparty: str | None = None
    link_key: str | None = None
    link_key_type: str | None = None
    extra: dict = field(default_factory=dict)


class LogDecoder(ABC):
    """Stateless decoder for a single event type."""

    @property
    @abstractmethod
    def topic0(self) -> str:
        """The keccak256 hash of the event signature."""
        ...

    @property
    @abstractmethod
    def event_signature(self) -> str:
        """Human-readable Solidity event signature."""
        ...

    @property
    @abstractmethod
    def protocol(self) -> str:
        """Protocol slug, e.g. 'uniswap_v3', 'across', 'stargate'."""
        ...

    @abstractmethod
    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        """Decode a raw log dict into a DecodedEvent.

        *log* contains: address, topics, data, blockNumber, transactionHash, logIndex.
        All hex strings are 0x-prefixed.
        """
        ...

    def _decode_abi(self, data_hex: str, types: list[str]) -> tuple[Any, ...]:
        """Helper: decode event data bytes via eth-abi."""
        if data_hex == "0x":
            return tuple()
        return eth_abi_decode(types, bytes.fromhex(data_hex[2:]))

    def _topic_address(self, topic: str | None) -> str:
        if topic is None:
            return ""
        return "0x" + topic[-40:].lower()
