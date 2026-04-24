from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Iterator

import httpx

from core.adapters.base import Adapter, CanonicalEvent


POSTHOG_API_BASE = "https://app.posthog.com"


class PostHogAdapter(Adapter):
    @property
    def source_system(self) -> str:
        return "posthog"

    def __init__(
        self,
        api_key: str,
        project_id: str,
        host: str = POSTHOG_API_BASE,
    ) -> None:
        self.api_key = api_key
        self.project_id = project_id
        self.host = host.rstrip("/")
        self._client: httpx.Client | None = None

    def _ensure_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=30.0,
                headers={"Authorization": f"Bearer {self.api_key}"},
            )
        return self._client

    def _fetch_events(
        self,
        start: datetime,
        end: datetime,
        after: str | None = None,
    ) -> dict[str, Any]:
        url = f"{self.host}/api/projects/{self.project_id}/events"
        params: dict[str, Any] = {
            "after": start.isoformat(),
            "before": end.isoformat(),
            "limit": 100,
        }
        if after:
            params["after"] = after

        client = self._ensure_client()
        resp = client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def ingest(self, start: datetime, end: datetime) -> Iterator[CanonicalEvent]:
        cursor: str | None = None
        row_index = 0

        while True:
            data = self._fetch_events(start, end, after=cursor)
            results = data.get("results", [])
            if not results:
                break

            for event in results:
                ev = self._parse_event(event, row_index)
                if ev is not None:
                    yield ev
                row_index += 1

            next_url = data.get("next")
            if not next_url:
                break
            cursor = _extract_cursor(next_url)
            if cursor is None:
                break

    def _parse_event(
        self,
        event: dict[str, Any],
        row_index: int,
    ) -> CanonicalEvent | None:
        distinct_id = event.get("distinct_id", "")
        event_name = event.get("event", "")
        timestamp_raw = event.get("timestamp", "")
        properties = event.get("properties", {}) or {}

        if not distinct_id or not event_name:
            return None

        event_type = _map_event_type(event_name)
        if event_type is None:
            return None

        timestamp = _parse_timestamp(timestamp_raw)
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        source_event_id = hashlib.sha256(
            f"{distinct_id}:{event_name}:{timestamp_raw}:{row_index}".encode()
        ).hexdigest()[:32]

        event_id = hashlib.sha256(f"{self.source_system}:{source_event_id}".encode()).hexdigest()

        extra: dict[str, Any] = {}
        session_id = properties.get("$session_id")
        if session_id:
            extra["session_id"] = session_id
        if properties.get("$current_url"):
            extra["url"] = properties["$current_url"]
        if properties.get("$referrer"):
            extra["referrer"] = properties["$referrer"]
        if properties.get("utm_source"):
            extra["utm_source"] = properties["utm_source"]
        if properties.get("utm_medium"):
            extra["utm_medium"] = properties["utm_medium"]

        return CanonicalEvent(
            entity_id=distinct_id,
            entity_type="device_id",
            event_id=event_id,
            event_type=event_type,
            event_category="acquisition",
            timestamp=timestamp,
            source_system=self.source_system,
            source_event_id=source_event_id,
            extra=extra if extra else None,
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()


def _map_event_type(event_name: str) -> str | None:
    mapping = {
        "$pageview": "pageview",
        "$session_start": "session_start",
        "$autocapture": "autocapture",
    }
    return mapping.get(event_name)


def _parse_timestamp(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _extract_cursor(next_url: str) -> str | None:
    from urllib.parse import parse_qs, urlparse

    parsed = urlparse(next_url)
    query = parse_qs(parsed.query)
    cursors = query.get("after")
    if cursors:
        return cursors[0]
    return None
