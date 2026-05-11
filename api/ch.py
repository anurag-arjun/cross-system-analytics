"""ClickHouse client factory — reads connection params from env."""

from __future__ import annotations

import os
from functools import lru_cache

import clickhouse_connect


@lru_cache(maxsize=1)
def get_client():
    """Single shared client. clickhouse-connect is thread-safe; FastAPI is
    single-event-loop, so one client across requests is fine."""
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8124")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "nexus"),
        database=os.environ.get("CLICKHOUSE_DB", "nexus"),
    )


def rows_to_dicts(result) -> list[dict]:
    """Convert a clickhouse-connect QueryResult to a list of dicts."""
    cols = result.column_names
    return [dict(zip(cols, row)) for row in result.result_rows]
