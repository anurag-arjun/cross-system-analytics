"""Minimal Dune Query API client.

Just enough to run a SQL query, poll for completion, and stream pages of
results back. Tracks bytes returned + execution time so callers can
enforce free-tier credit budgets.

Free-tier accounting (verified 2026-05-08): 2,500 credits/month, 20
credits per MB exported. The `total_result_set_bytes` field on each
execution's metadata is the authoritative byte count for the query.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Iterator

import httpx

logger = logging.getLogger(__name__)

DEFAULT_BASE = "https://api.dune.com"
TERMINAL_STATES = {"QUERY_STATE_COMPLETED", "QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"}


class DuneError(RuntimeError):
    pass


@dataclass
class ExecutionResult:
    rows: list[dict[str, Any]]
    column_names: list[str]
    column_types: list[str]
    row_count: int
    result_set_bytes: int
    total_result_set_bytes: int
    execution_time_ms: int


@dataclass
class BudgetTracker:
    """Sums bytes + ms across multiple executions in one bootstrap run."""

    total_bytes: int = 0
    total_execution_ms: int = 0
    queries: int = 0
    per_query: list[dict[str, Any]] = field(default_factory=list)

    def record(self, label: str, result: ExecutionResult) -> None:
        self.queries += 1
        self.total_bytes += result.total_result_set_bytes
        self.total_execution_ms += result.execution_time_ms
        self.per_query.append(
            dict(
                label=label,
                rows=result.row_count,
                bytes=result.total_result_set_bytes,
                ms=result.execution_time_ms,
            )
        )

    def estimate_credits(self, credits_per_mb: int = 20) -> float:
        """Free-tier costs 20 credits/MB exported. Plus tier is 2."""
        return credits_per_mb * (self.total_bytes / (1024 * 1024))


class DuneClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE,
        poll_interval_s: float = 3.0,
        timeout_s: float = 600.0,
    ) -> None:
        self._api_key = api_key
        self._base = base_url.rstrip("/")
        self._poll = poll_interval_s
        self._timeout = timeout_s
        self._client = httpx.Client(
            headers={"X-Dune-Api-Key": api_key},
            timeout=60.0,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "DuneClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def execute(self, sql: str, performance: str = "small") -> str:
        """POST /v1/sql/execute. Returns execution_id."""
        resp = self._client.post(
            f"{self._base}/api/v1/sql/execute",
            json={"sql": sql, "performance": performance},
        )
        resp.raise_for_status()
        data = resp.json()
        if "execution_id" not in data:
            raise DuneError(f"unexpected execute response: {data}")
        return data["execution_id"]

    def wait(self, execution_id: str) -> str:
        """Poll status until terminal. Returns final state."""
        deadline = time.monotonic() + self._timeout
        while time.monotonic() < deadline:
            resp = self._client.get(
                f"{self._base}/api/v1/execution/{execution_id}/status"
            )
            resp.raise_for_status()
            state = resp.json().get("state", "")
            if state in TERMINAL_STATES:
                return state
            time.sleep(self._poll)
        raise DuneError(f"execution {execution_id} timed out after {self._timeout}s")

    def results(self, execution_id: str, limit: int = 10000, offset: int = 0) -> dict[str, Any]:
        """GET /v1/execution/{id}/results. Returns the parsed JSON."""
        resp = self._client.get(
            f"{self._base}/api/v1/execution/{execution_id}/results",
            params={"limit": limit, "offset": offset},
        )
        resp.raise_for_status()
        return resp.json()

    def execute_sql(
        self, sql: str, performance: str = "small", page_size: int = 10000
    ) -> ExecutionResult:
        """Run a query end-to-end and fetch all pages.

        Dune paginates results at 10k rows by default. For results that span
        multiple pages we keep fetching until a page comes back empty,
        accumulating rows into one ExecutionResult. The `total_result_set_bytes`
        in the metadata is authoritative (server-side), unaffected by paging.
        """
        eid = self.execute(sql, performance=performance)
        state = self.wait(eid)
        if state != "QUERY_STATE_COMPLETED":
            results = self.results(eid)
            raise DuneError(f"execution {eid} ended in state {state}: {results}")

        all_rows: list[dict[str, Any]] = []
        offset = 0
        meta: dict[str, Any] = {}
        while True:
            body = self.results(eid, limit=page_size, offset=offset)
            result = body.get("result", {})
            page_rows = result.get("rows", [])
            if not meta:
                meta = result.get("metadata", {})
            all_rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            offset += len(page_rows)

        return ExecutionResult(
            rows=all_rows,
            column_names=meta.get("column_names", []),
            column_types=meta.get("column_types", []),
            row_count=len(all_rows),
            result_set_bytes=meta.get("result_set_bytes", 0),
            total_result_set_bytes=meta.get("total_result_set_bytes", 0),
            execution_time_ms=meta.get("execution_time_millis", 0),
        )

    def iter_rows(
        self,
        sql: str,
        page_size: int = 10000,
        performance: str = "small",
    ) -> Iterator[dict[str, Any]]:
        """Run a query and yield rows page by page. Use for results above ~10k rows."""
        eid = self.execute(sql, performance=performance)
        state = self.wait(eid)
        if state != "QUERY_STATE_COMPLETED":
            raise DuneError(f"execution {eid} ended in state {state}")
        offset = 0
        while True:
            body = self.results(eid, limit=page_size, offset=offset)
            rows = body.get("result", {}).get("rows", [])
            if not rows:
                return
            yield from rows
            if len(rows) < page_size:
                return
            offset += len(rows)

    def count(self, inner_sql: str) -> int:
        """Run `SELECT COUNT(*) FROM (inner_sql)` for pre-flight sizing."""
        wrapped = f"SELECT COUNT(*) AS n FROM ({inner_sql.rstrip(';')}) _"
        result = self.execute_sql(wrapped)
        return int(result.rows[0]["n"]) if result.rows else 0
