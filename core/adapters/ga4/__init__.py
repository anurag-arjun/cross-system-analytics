"""Google Analytics 4 adapter (OAuth, read-only)."""

from datetime import datetime
from typing import Iterator

from core.adapters.base import Adapter, CanonicalEvent


class GA4Adapter(Adapter):
    @property
    def source_system(self) -> str:
        return "ga4"

    def __init__(self, property_id: str, credentials_path: str):
        self.property_id = property_id
        self.credentials_path = credentials_path

    def ingest(self, start: datetime, end: datetime) -> Iterator[CanonicalEvent]:
        # TODO: OAuth to GA4 Data API, fetch sessions + pageviews
        # Map ga4_client_id to entity_id
        yield from []
