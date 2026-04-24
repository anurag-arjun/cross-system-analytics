"""Base adapter interface for all ingestion adapters."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Iterator
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass
class CanonicalEvent:
    entity_id: str
    entity_type: str
    event_id: str
    event_type: str
    event_category: str
    timestamp: datetime
    source_system: str
    source_event_id: str
    chain: Optional[str] = None
    block_number: Optional[int] = None
    block_time: Optional[datetime] = None
    tx_hash: Optional[str] = None
    log_index: Optional[int] = None
    protocol: Optional[str] = None
    venue: Optional[str] = None
    token_in: Optional[str] = None
    token_out: Optional[str] = None
    amount_in: Optional[Decimal] = None
    amount_out: Optional[Decimal] = None
    amount_in_usd: Optional[Decimal] = None
    amount_out_usd: Optional[Decimal] = None
    counterparty: Optional[str] = None
    aggregator: Optional[str] = None
    link_key: Optional[str] = None
    link_key_type: Optional[str] = None
    extra: Optional[dict] = None


class Adapter(ABC):
    @property
    @abstractmethod
    def source_system(self) -> str: ...

    @abstractmethod
    def ingest(self, start: datetime, end: datetime) -> Iterator[CanonicalEvent]: ...

    def validate(self, event: CanonicalEvent) -> bool:
        from core.schemas.validator import validate_event

        return validate_event(event)
