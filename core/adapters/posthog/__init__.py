"""PostHog adapter (API key)."""

from datetime import datetime
from typing import Iterator

from core.adapters.base import Adapter, CanonicalEvent


class PostHogAdapter(Adapter):
    @property
    def source_system(self) -> str:
        return "posthog"

    def __init__(
        self, api_key: str, project_id: str, host: str = "https://app.posthog.com"
    ):
        self.api_key = api_key
        self.project_id = project_id
        self.host = host

    def ingest(self, start: datetime, end: datetime) -> Iterator[CanonicalEvent]:
        # TODO: PostHog API for events, pageviews, $autocapture
        # Map distinct_id to entity_id
        yield from []
