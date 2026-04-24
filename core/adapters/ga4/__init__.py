from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Iterator

import httpx
from google.auth import jwt
from google.oauth2 import service_account

from core.adapters.base import Adapter, CanonicalEvent


GA4_API_BASE = "https://analyticsdata.googleapis.com/v1beta"
GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]


class GA4Adapter(Adapter):
    @property
    def source_system(self) -> str:
        return "ga4"

    def __init__(
        self,
        property_id: str,
        credentials_path: str,
        api_base: str = GA4_API_BASE,
    ) -> None:
        self.property_id = property_id
        self.credentials_path = credentials_path
        self.api_base = api_base
        self._credentials: service_account.Credentials | None = None
        self._client: httpx.Client | None = None

    def _ensure_credentials(self) -> service_account.Credentials:
        if self._credentials is None:
            self._credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=GA4_SCOPES,
            )
        return self._credentials

    def _ensure_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(timeout=30.0)
        return self._client

    def _get_access_token(self) -> str:
        creds = self._ensure_credentials()
        if creds.expired or not creds.token:
            creds.refresh(jwt.Request())
        return creds.token

    def _run_report(
        self,
        start_date: str,
        end_date: str,
        offset: int = 0,
    ) -> dict[str, Any]:
        token = self._get_access_token()
        url = f"{self.api_base}/properties/{self.property_id}:runReport"

        payload = {
            "dateRanges": [{"startDate": start_date, "endDate": end_date}],
            "dimensions": [
                {"name": "dateHourMinute"},
                {"name": "eventName"},
                {"name": "sessionId"},
                {"name": "clientId"},
                {"name": "pageLocation"},
                {"name": "pageReferrer"},
                {"name": "sessionSource"},
                {"name": "sessionMedium"},
            ],
            "metrics": [{"name": "eventCount"}],
            "limit": 100_000,
            "offset": str(offset),
        }

        client = self._ensure_client()
        resp = client.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    def ingest(self, start: datetime, end: datetime) -> Iterator[CanonicalEvent]:
        start_date = start.strftime("%Y-%m-%d")
        end_date = end.strftime("%Y-%m-%d")

        offset = 0
        row_count = 0
        while True:
            data = self._run_report(start_date, end_date, offset=offset)
            rows = data.get("rows", [])
            if not rows:
                break

            dimensions = [d["name"] for d in data.get("dimensionHeaders", [])]

            for idx, row in enumerate(rows):
                ev = self._parse_row(
                    dimensions,
                    row,
                    row_index=row_count + idx,
                )
                if ev is not None:
                    yield ev

            row_count += len(rows)
            total_rows = int(data.get("rowCount", 0))
            if offset + len(rows) >= total_rows:
                break
            offset += 100_000

    def _parse_row(
        self,
        dimensions: list[str],
        row: dict[str, Any],
        row_index: int,
    ) -> CanonicalEvent | None:
        vals = {
            name: dv.get("value", "")
            for name, dv in zip(dimensions, row.get("dimensionValues", []))
        }

        event_name = vals.get("eventName", "")
        client_id = vals.get("clientId", "")
        session_id = vals.get("sessionId", "")
        date_hour_minute = vals.get("dateHourMinute", "")

        if not event_name or not client_id:
            return None

        event_type = _map_event_type(event_name)
        if event_type is None:
            return None

        timestamp = _parse_date_hour_minute(date_hour_minute)
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        source_event_id = hashlib.sha256(
            f"{client_id}:{session_id}:{event_name}:{date_hour_minute}:{row_index}".encode()
        ).hexdigest()[:32]

        event_id = hashlib.sha256(f"{self.source_system}:{source_event_id}".encode()).hexdigest()

        extra: dict[str, Any] = {}
        if session_id:
            extra["session_id"] = session_id
        if vals.get("pageLocation"):
            extra["url"] = vals["pageLocation"]
        if vals.get("pageReferrer"):
            extra["referrer"] = vals["pageReferrer"]
        if vals.get("sessionSource"):
            extra["utm_source"] = vals["sessionSource"]
        if vals.get("sessionMedium"):
            extra["utm_medium"] = vals["sessionMedium"]

        return CanonicalEvent(
            entity_id=client_id,
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
        "session_start": "session_start",
        "page_view": "pageview",
        "pageview": "pageview",
        "first_visit": "first_visit",
        "user_engagement": "user_engagement",
    }
    return mapping.get(event_name)


def _parse_date_hour_minute(value: str) -> datetime | None:
    if len(value) != 12:
        return None
    try:
        year = int(value[:4])
        month = int(value[4:6])
        day = int(value[6:8])
        hour = int(value[8:10])
        minute = int(value[10:12])
        return datetime(year, month, day, hour, minute, 0, tzinfo=timezone.utc)
    except (ValueError, IndexError):
        return None
