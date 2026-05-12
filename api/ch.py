"""ClickHouse client factory — one client per thread.

Earlier comment claimed "FastAPI is single-event-loop, so one client across
requests is fine" — that's wrong. FastAPI runs `def` (sync) endpoint handlers
in a threadpool via anyio.to_thread, so multiple concurrent requests hit the
same clickhouse-connect client and trip the "Attempt to execute concurrent
queries within the same session" guard. Use threading.local so each worker
thread gets its own client.
"""

from __future__ import annotations

import os
import threading

import clickhouse_connect

_local = threading.local()


def get_client():
    """Return the calling thread's client, creating it lazily."""
    if not hasattr(_local, "client") or _local.client is None:
        _local.client = clickhouse_connect.get_client(
            host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
            port=int(os.environ.get("CLICKHOUSE_PORT", "8124")),
            username=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", "nexus"),
            database=os.environ.get("CLICKHOUSE_DB", "nexus"),
        )
    return _local.client


def rows_to_dicts(result) -> list[dict]:
    """Convert a clickhouse-connect QueryResult to a list of dicts."""
    cols = result.column_names
    return [dict(zip(cols, row)) for row in result.result_rows]
