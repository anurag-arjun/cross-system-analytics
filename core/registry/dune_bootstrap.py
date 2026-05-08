"""Bootstrap export from Dune Analytics into protocol_contracts + contract_labels.

Strategy:

1. Pull DEX pool addresses from `dex.trades` over the past N days for the
   in-scope chains. Dedup on (blockchain, project, version, address). Rows
   merged into `protocol_contracts` with source='dune'.

2. Same shape against `dex_aggregator.trades` (router/proxy contracts) and
   `bridges_evms.deposits` / `bridges_evms.withdrawals` (bridge endpoints).

3. Labels via JOIN against the address universe assembled in (1) + (2).
   *Never* SELECT directly from `labels.addresses` — that table is ~1.3B
   rows across 5 chains, would blow the free-tier credit budget. Filtering
   to addresses we've already seen keeps the cost bounded.

Pre-flight: every step runs a `SELECT COUNT(*)` first, projects bytes from
a sampled bytes-per-row constant, and aborts if the cumulative budget for
the run would exceed `byte_cap` (default 50 MB).

Idempotent: every upsert uses `ON CONFLICT (chain, address, source) DO UPDATE`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from core.registry.contract_labels import ContractLabel, ContractLabelStore
from core.registry.dune import BudgetTracker, DuneClient, ExecutionResult
from core.registry.protocol_contracts import (
    ProtocolContract,
    ProtocolContractStore,
)

logger = logging.getLogger(__name__)

IN_SCOPE_CHAINS = ("ethereum", "base", "arbitrum", "optimism", "polygon")
DEFAULT_BYTE_CAP = 50 * 1024 * 1024  # 50 MB
ESTIMATED_BYTES_PER_ROW = 80  # observed: ~64 bytes for the dex.trades shape

# Dune dataset table names — all use `block_time` and `blockchain` columns
# in canonical form.
DEX_TRADES = "dex.trades"
DEX_AGG_TRADES = "dex_aggregator.trades"
BRIDGES_DEPOSITS = "bridges_evms.deposits"
BRIDGES_WITHDRAWALS = "bridges_evms.withdrawals"


@dataclass
class BootstrapStats:
    contracts_upserted: int = 0
    labels_upserted: int = 0
    distinct_protocols: set[str] = field(default_factory=set)
    distinct_chains: set[str] = field(default_factory=set)
    aborted_at: str | None = None
    budget: BudgetTracker = field(default_factory=BudgetTracker)


# ----------------------------------------------------------------------
# SQL builders
# ----------------------------------------------------------------------


def _chains_in_clause(chains: tuple[str, ...]) -> str:
    return ", ".join(f"'{c}'" for c in chains)


def _dex_contracts_query(table: str, chains: tuple[str, ...], days: int) -> str:
    """Distinct (blockchain, project, version, project_contract_address).

    Used for `dex.trades` and `dex_aggregator.trades` which share the
    Spellbook canonical column layout.
    """
    return f"""
        SELECT
            blockchain,
            project,
            COALESCE(CAST(version AS VARCHAR), '') AS version,
            project_contract_address
        FROM {table}
        WHERE blockchain IN ({_chains_in_clause(chains)})
          AND block_time > now() - interval '{days}' day
          AND project_contract_address IS NOT NULL
        GROUP BY 1, 2, 3, 4
    """


def _bridges_contracts_query(
    table: str,
    chains: tuple[str, ...],
    days: int,
    chain_col: str,
) -> str:
    """Distinct (chain, bridge, version, contract_address) for the bridges schema.

    `bridges_evms.deposits` uses `deposit_chain`, `withdrawals` uses
    `withdrawal_chain`. The contract is whichever endpoint emitted the event
    on this chain — we just want the address universe to feed protocol_contracts.
    """
    return f"""
        SELECT
            {chain_col} AS blockchain,
            bridge_name AS project,
            COALESCE(CAST(bridge_version AS VARCHAR), '') AS version,
            contract_address AS project_contract_address
        FROM {table}
        WHERE {chain_col} IN ({_chains_in_clause(chains)})
          AND block_time > now() - interval '{days}' day
          AND contract_address IS NOT NULL
        GROUP BY 1, 2, 3, 4
    """


def _labels_join_query(chains: tuple[str, ...], days: int) -> str:
    """Pull labels.addresses rows for addresses we have seen in dex/agg/bridges.

    Uses CONCAT('0x', SUBSTR(...)) trick to convert the varbinary `address`
    in labels.addresses to the 0x-prefixed hex string used elsewhere — the
    table column is varbinary, but Dune emits 0x-strings in JSON results.
    """
    chains_in = _chains_in_clause(chains)
    return f"""
        WITH known AS (
            SELECT DISTINCT blockchain, project_contract_address AS address
            FROM {DEX_TRADES}
            WHERE blockchain IN ({chains_in})
              AND block_time > now() - interval '{days}' day
              AND project_contract_address IS NOT NULL
            UNION
            SELECT DISTINCT blockchain, project_contract_address AS address
            FROM {DEX_AGG_TRADES}
            WHERE blockchain IN ({chains_in})
              AND block_time > now() - interval '{days}' day
              AND project_contract_address IS NOT NULL
            UNION
            SELECT DISTINCT deposit_chain AS blockchain, contract_address AS address
            FROM {BRIDGES_DEPOSITS}
            WHERE deposit_chain IN ({chains_in})
              AND block_time > now() - interval '{days}' day
              AND contract_address IS NOT NULL
        )
        SELECT
            k.blockchain AS blockchain,
            k.address    AS address,
            l.name       AS name,
            l.category   AS category
        FROM known k
        JOIN labels.addresses l
            ON l.blockchain = k.blockchain
           AND l.address    = k.address
        WHERE l.name IS NOT NULL
    """


# ----------------------------------------------------------------------
# Result -> ProtocolContract / ContractLabel adapters
# ----------------------------------------------------------------------


def _row_to_contract(row: dict[str, Any]) -> ProtocolContract | None:
    chain = (row.get("blockchain") or "").strip().lower()
    address = (row.get("project_contract_address") or "").strip()
    protocol = (row.get("project") or "").strip()
    if not (chain and address and protocol):
        return None
    return ProtocolContract(
        chain=chain,
        address=address.lower(),
        protocol=protocol,
        version=(row.get("version") or "").strip() or None,
        contract_type=None,
        source="dune",
    )


def _row_to_label(row: dict[str, Any]) -> ContractLabel | None:
    chain = (row.get("blockchain") or "").strip().lower()
    address = (row.get("address") or "").strip()
    name = (row.get("name") or "").strip()
    if not (chain and address and name):
        return None
    return ContractLabel(
        chain=chain,
        address=address.lower(),
        label=name,
        category=(row.get("category") or "").strip() or None,
        source="dune",
    )


# ----------------------------------------------------------------------
# Phases — each runs a pre-flight count before any data export
# ----------------------------------------------------------------------


def _check_budget(
    label: str,
    expected_rows: int,
    budget: BudgetTracker,
    byte_cap: int,
) -> bool:
    projected_bytes = expected_rows * ESTIMATED_BYTES_PER_ROW
    cumulative = budget.total_bytes + projected_bytes
    if cumulative > byte_cap:
        logger.warning(
            "[%s] projected bytes (%d) + already-spent (%d) = %d exceeds cap %d; aborting",
            label,
            projected_bytes,
            budget.total_bytes,
            cumulative,
            byte_cap,
        )
        return False
    logger.info(
        "[%s] %d rows projected (~%.2f MB); cumulative ~%.2f MB / %.2f MB cap",
        label,
        expected_rows,
        projected_bytes / 1024 / 1024,
        cumulative / 1024 / 1024,
        byte_cap / 1024 / 1024,
    )
    return True


def _run_contracts_phase(
    client: DuneClient,
    label: str,
    sql: str,
    inner_sql: str,
    contract_store: ProtocolContractStore,
    stats: BootstrapStats,
    byte_cap: int,
) -> bool:
    """Runs pre-flight count, then full export, then upserts.

    Returns False if budget cap aborted the phase (sets stats.aborted_at).
    """
    expected_rows = client.count(inner_sql)
    if not _check_budget(label, expected_rows, stats.budget, byte_cap):
        stats.aborted_at = label
        return False
    if expected_rows == 0:
        logger.info("[%s] 0 rows; skipping export", label)
        return True

    result = client.execute_sql(sql)
    stats.budget.record(label, result)

    contracts = []
    for row in result.rows:
        c = _row_to_contract(row)
        if c is None:
            continue
        contracts.append(c)
        stats.distinct_protocols.add(c.protocol)
        stats.distinct_chains.add(c.chain)
    n = contract_store.upsert_many(contracts)
    stats.contracts_upserted += n
    logger.info("[%s] upserted %d contracts (%d rows from Dune)", label, n, result.row_count)
    return True


def _run_labels_phase(
    client: DuneClient,
    sql: str,
    inner_sql: str,
    label_store: ContractLabelStore,
    stats: BootstrapStats,
    byte_cap: int,
) -> bool:
    expected_rows = client.count(inner_sql)
    if not _check_budget("labels", expected_rows, stats.budget, byte_cap):
        stats.aborted_at = "labels"
        return False
    if expected_rows == 0:
        logger.info("[labels] 0 rows; skipping export")
        return True
    result = client.execute_sql(sql)
    stats.budget.record("labels", result)

    labels = []
    for row in result.rows:
        la = _row_to_label(row)
        if la is None:
            continue
        labels.append(la)
    n = label_store.upsert_many(labels)
    stats.labels_upserted += n
    logger.info("[labels] upserted %d labels (%d rows from Dune)", n, result.row_count)
    return True


# ----------------------------------------------------------------------
# Orchestrator
# ----------------------------------------------------------------------


def run_bootstrap(
    client: DuneClient,
    contract_store: ProtocolContractStore,
    label_store: ContractLabelStore | None = None,
    *,
    chains: tuple[str, ...] = IN_SCOPE_CHAINS,
    days: int = 90,
    byte_cap: int = DEFAULT_BYTE_CAP,
    skip_labels: bool = False,
) -> BootstrapStats:
    """Execute all bootstrap phases. Aborts if cumulative bytes exceed cap."""
    stats = BootstrapStats()

    phases = [
        ("dex.trades", _dex_contracts_query(DEX_TRADES, chains, days)),
        ("dex_aggregator.trades", _dex_contracts_query(DEX_AGG_TRADES, chains, days)),
        (
            "bridges_evms.deposits",
            _bridges_contracts_query(BRIDGES_DEPOSITS, chains, days, "deposit_chain"),
        ),
        (
            "bridges_evms.withdrawals",
            _bridges_contracts_query(
                BRIDGES_WITHDRAWALS, chains, days, "withdrawal_chain"
            ),
        ),
    ]

    for label, sql in phases:
        ok = _run_contracts_phase(
            client, label, sql, sql, contract_store, stats, byte_cap
        )
        if not ok:
            return stats

    if label_store is not None and not skip_labels:
        sql = _labels_join_query(chains, days)
        _run_labels_phase(client, sql, sql, label_store, stats, byte_cap)

    return stats
