"""Tests for the Dune bootstrap orchestrator.

The Dune client is mocked. An opt-in end-to-end test runs against the
real Dune API if DUNE_API_KEY is set; we only run a tiny query to keep
credit usage trivial.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

from core.registry.contract_labels import (
    ContractLabel,
    InMemoryContractLabelStore,
)
from core.registry.dune import BudgetTracker, DuneClient, ExecutionResult
from core.registry.dune_bootstrap import (
    BootstrapStats,
    _row_to_contract,
    _row_to_label,
    run_bootstrap,
)
from core.registry.protocol_contracts import (
    InMemoryProtocolContractStore,
    ProtocolContract,
)


# ---------------------------------------------------------------------------
# Row -> dataclass adapters
# ---------------------------------------------------------------------------


def test_row_to_contract_normalizes_and_filters():
    valid = _row_to_contract(
        {
            "blockchain": "Base",
            "project": "uniswap",
            "version": "3",
            "project_contract_address": "0xABCDEF0000000000000000000000000000000001",
        }
    )
    assert valid is not None
    assert valid.chain == "base"
    assert valid.address == "0xabcdef0000000000000000000000000000000001"
    assert valid.protocol == "uniswap"
    assert valid.version == "3"
    assert valid.source == "dune"

    # Missing required fields
    assert _row_to_contract({"blockchain": "base"}) is None
    assert (
        _row_to_contract(
            {"blockchain": "base", "project": "uniswap", "project_contract_address": ""}
        )
        is None
    )


def test_row_to_label_normalizes():
    la = _row_to_label(
        {
            "blockchain": "Polygon",
            "address": "0xCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC",
            "name": "QuickSwap Router",
            "category": "dex",
        }
    )
    assert la is not None
    assert la.chain == "polygon"
    assert la.address == "0xcccccccccccccccccccccccccccccccccccccccc"
    assert la.label == "QuickSwap Router"
    assert la.category == "dex"
    assert la.source == "dune"


# ---------------------------------------------------------------------------
# Budget tracker
# ---------------------------------------------------------------------------


def test_budget_tracker_accumulates():
    bt = BudgetTracker()
    bt.record(
        "phase1",
        ExecutionResult(
            rows=[],
            column_names=[],
            column_types=[],
            row_count=0,
            result_set_bytes=1024 * 1024,
            total_result_set_bytes=1024 * 1024,
            execution_time_ms=200,
        ),
    )
    bt.record(
        "phase2",
        ExecutionResult(
            rows=[],
            column_names=[],
            column_types=[],
            row_count=0,
            result_set_bytes=2 * 1024 * 1024,
            total_result_set_bytes=2 * 1024 * 1024,
            execution_time_ms=300,
        ),
    )
    assert bt.queries == 2
    assert bt.total_bytes == 3 * 1024 * 1024
    assert bt.total_execution_ms == 500
    # 3 MB at 20 credits/MB = 60 credits free tier; 6 credits plus tier
    assert bt.estimate_credits() == 60.0
    assert bt.estimate_credits(2) == 6.0


# ---------------------------------------------------------------------------
# Orchestrator with a mocked DuneClient
# ---------------------------------------------------------------------------


class FakeDuneClient:
    """In-process Dune stand-in. Returns canned per-query results."""

    def __init__(self, count_by_phase: dict[str, int], rows_by_phase: dict[str, list[dict]]):
        self._counts = count_by_phase
        self._rows = rows_by_phase
        self._call_log: list[str] = []

    def _identify(self, sql: str) -> str:
        # Iterates in insertion order. List `labels.addresses` first in the
        # dict so the labels JOIN query (which UNIONs over multiple tables)
        # is identified by its JOIN target, not by a UNION subquery table.
        for key in self._counts:
            if key in sql:
                return key
        raise AssertionError(f"no canned response for SQL containing one of {list(self._counts)}: {sql[:80]}")

    def count(self, inner_sql: str) -> int:
        key = self._identify(inner_sql)
        self._call_log.append(f"count:{key}")
        return self._counts[key]

    def execute_sql(self, sql: str, performance: str = "small") -> ExecutionResult:
        key = self._identify(sql)
        self._call_log.append(f"execute:{key}")
        rows = self._rows[key]
        return ExecutionResult(
            rows=rows,
            column_names=list(rows[0].keys()) if rows else [],
            column_types=[],
            row_count=len(rows),
            result_set_bytes=len(rows) * 80,
            total_result_set_bytes=len(rows) * 80,
            execution_time_ms=10,
        )


def test_run_bootstrap_happy_path():
    fake = FakeDuneClient(
        count_by_phase={
            "labels.addresses": 1,
            "dex.trades": 2,
            "dex_aggregator.trades": 1,
            "bridges_evms.deposits": 1,
            "bridges_evms.withdrawals": 0,
        },
        rows_by_phase={
            "dex.trades": [
                {
                    "blockchain": "ethereum",
                    "project": "uniswap",
                    "version": "3",
                    "project_contract_address": "0x" + "aa" * 20,
                },
                {
                    "blockchain": "base",
                    "project": "aerodrome",
                    "version": "1",
                    "project_contract_address": "0x" + "bb" * 20,
                },
            ],
            "dex_aggregator.trades": [
                {
                    "blockchain": "ethereum",
                    "project": "1inch",
                    "version": "5",
                    "project_contract_address": "0x" + "cc" * 20,
                }
            ],
            "bridges_evms.deposits": [
                {
                    "blockchain": "polygon",
                    "project": "polygon_pos",
                    "version": "1",
                    "project_contract_address": "0x" + "dd" * 20,
                }
            ],
            "bridges_evms.withdrawals": [],
            "labels.addresses": [
                {
                    "blockchain": "ethereum",
                    "address": "0x" + "aa" * 20,
                    "name": "Uniswap V3 Pool",
                    "category": "dex",
                }
            ],
        },
    )

    cstore = InMemoryProtocolContractStore()
    lstore = InMemoryContractLabelStore()

    stats = run_bootstrap(
        fake,  # type: ignore[arg-type]
        cstore,
        lstore,
        chains=("ethereum", "base", "polygon"),
        days=7,
    )

    assert stats.aborted_at is None
    assert stats.contracts_upserted == 4
    assert stats.labels_upserted == 1
    assert stats.distinct_protocols == {"uniswap", "aerodrome", "1inch", "polygon_pos"}
    assert stats.distinct_chains == {"ethereum", "base", "polygon"}
    assert cstore.lookup("ethereum", "0x" + "aa" * 20) == "uniswap"
    assert lstore.lookup("ethereum", "0x" + "aa" * 20) == "Uniswap V3 Pool"


def test_run_bootstrap_aborts_when_cap_exceeded():
    """A first phase that fits gets imported; the next phase that would
    exceed the cap aborts the run and stats.aborted_at is set."""
    fake = FakeDuneClient(
        count_by_phase={
            "dex.trades": 100,            # 100 * 80 = 8000 bytes, fits
            "dex_aggregator.trades": 1_000_000,  # 80 MB projected, blows cap
            "bridges_evms.deposits": 0,
            "bridges_evms.withdrawals": 0,
            "labels.addresses": 0,
        },
        rows_by_phase={
            "dex.trades": [
                {
                    "blockchain": "ethereum",
                    "project": "uniswap",
                    "version": "3",
                    "project_contract_address": "0x" + "aa" * 20,
                }
            ],
            "dex_aggregator.trades": [],
            "bridges_evms.deposits": [],
            "bridges_evms.withdrawals": [],
            "labels.addresses": [],
        },
    )

    cstore = InMemoryProtocolContractStore()
    lstore = InMemoryContractLabelStore()

    stats = run_bootstrap(
        fake,  # type: ignore[arg-type]
        cstore,
        lstore,
        chains=("ethereum",),
        days=7,
        byte_cap=50 * 1024 * 1024,  # 50 MB
    )

    # First phase succeeded; abort happened on the second
    assert stats.contracts_upserted == 1  # from dex.trades only
    assert stats.aborted_at == "dex_aggregator.trades"
    assert "execute:dex.trades" in fake._call_log
    assert "execute:dex_aggregator.trades" not in fake._call_log


def test_run_bootstrap_skip_labels():
    fake = FakeDuneClient(
        count_by_phase={
            "dex.trades": 1,
            "dex_aggregator.trades": 0,
            "bridges_evms.deposits": 0,
            "bridges_evms.withdrawals": 0,
        },
        rows_by_phase={
            "dex.trades": [
                {
                    "blockchain": "base",
                    "project": "uniswap",
                    "version": "3",
                    "project_contract_address": "0x" + "ee" * 20,
                }
            ],
            "dex_aggregator.trades": [],
            "bridges_evms.deposits": [],
            "bridges_evms.withdrawals": [],
        },
    )
    cstore = InMemoryProtocolContractStore()
    lstore = InMemoryContractLabelStore()
    stats = run_bootstrap(
        fake,  # type: ignore[arg-type]
        cstore,
        lstore,
        chains=("base",),
        days=7,
        skip_labels=True,
    )
    assert stats.contracts_upserted == 1
    assert stats.labels_upserted == 0
    assert all("labels" not in c for c in fake._call_log)


# ---------------------------------------------------------------------------
# Integration smoke test against the real Dune API (opt-in via DUNE_API_KEY)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not os.environ.get("DUNE_API_KEY"),
    reason="No DUNE_API_KEY in env",
)
def test_real_dune_executes_trivial_query():
    """Tiny query (~1 row, ~few bytes) — minimal credit cost."""
    with DuneClient(api_key=os.environ["DUNE_API_KEY"]) as client:
        result = client.execute_sql("SELECT 42 AS answer")
    assert result.row_count == 1
    assert result.rows[0]["answer"] == 42
    assert result.execution_time_ms > 0
