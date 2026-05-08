"""Storage for the (chain, address) -> protocol registry.

Two backends:

- ``InMemoryProtocolContractStore`` — a dict for tests, ad-hoc scripts, and
  small fixture sets.
- ``PostgresProtocolContractStore`` — production. Reads/writes
  ``protocol_contracts`` (see core/schemas/protocol_contracts.sql).

``make_resolver(store)`` returns the ``(chain, address) -> protocol | None``
callable expected by ``DecoderRegistry`` so the EVM adapter can use either
backend interchangeably.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Iterable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProtocolContract:
    chain: str
    address: str  # lowercase, 0x-prefixed
    protocol: str
    version: str | None = None
    contract_type: str | None = None
    source: str = "manual"

    def normalized(self) -> "ProtocolContract":
        """Return a copy with chain + address lowercased."""
        return ProtocolContract(
            chain=self.chain.lower(),
            address=self.address.lower(),
            protocol=self.protocol,
            version=self.version,
            contract_type=self.contract_type,
            source=self.source,
        )


class ProtocolContractStore(ABC):
    @abstractmethod
    def upsert_many(self, contracts: Iterable[ProtocolContract]) -> int:
        """Insert/update contracts. Returns the number of rows written."""

    @abstractmethod
    def lookup(self, chain: str, address: str) -> str | None:
        """Return the protocol for (chain, address), or None."""

    @abstractmethod
    def count(self) -> int:
        """Total rows in the store."""


# ----------------------------------------------------------------------
# In-memory backend
# ----------------------------------------------------------------------


class InMemoryProtocolContractStore(ProtocolContractStore):
    """Dict-backed store. Last-writer-wins on (chain, address, source)."""

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str, str], ProtocolContract] = {}

    def upsert_many(self, contracts: Iterable[ProtocolContract]) -> int:
        n = 0
        for c in contracts:
            cn = c.normalized()
            self._rows[(cn.chain, cn.address, cn.source)] = cn
            n += 1
        return n

    def lookup(self, chain: str, address: str) -> str | None:
        chain = chain.lower()
        address = address.lower()
        # Source-priority order: explicit -> dune -> spellbook -> other
        priority = ["manual", "dune", "spellbook"]
        candidates = [
            self._rows[k]
            for k in self._rows
            if k[0] == chain and k[1] == address
        ]
        if not candidates:
            return None
        for src in priority:
            for c in candidates:
                if c.source == src:
                    return c.protocol
        return candidates[0].protocol

    def count(self) -> int:
        return len(self._rows)

    def all_rows(self) -> list[ProtocolContract]:
        return list(self._rows.values())


# ----------------------------------------------------------------------
# Postgres backend
# ----------------------------------------------------------------------


class PostgresProtocolContractStore(ProtocolContractStore):
    """Postgres-backed store, expecting protocol_contracts.sql."""

    def __init__(self, dsn: str) -> None:
        import psycopg2  # noqa: F401  (import-time dependency check)

        self._dsn = dsn

    def _conn(self) -> Any:
        import psycopg2

        return psycopg2.connect(self._dsn)

    def upsert_many(self, contracts: Iterable[ProtocolContract]) -> int:
        rows = [
            (
                c.normalized().chain,
                c.normalized().address,
                c.protocol,
                c.version,
                c.contract_type,
                c.source,
            )
            for c in contracts
        ]
        if not rows:
            return 0
        sql = """
            INSERT INTO protocol_contracts
                (chain, address, protocol, version, contract_type, source)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (chain, address, source) DO UPDATE SET
                protocol = EXCLUDED.protocol,
                version = EXCLUDED.version,
                contract_type = EXCLUDED.contract_type,
                added_at = now()
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        return len(rows)

    def lookup(self, chain: str, address: str) -> str | None:
        sql = """
            SELECT protocol FROM protocol_contracts
            WHERE chain = %s AND address = %s
            ORDER BY CASE source
                WHEN 'manual'    THEN 1
                WHEN 'dune'      THEN 2
                WHEN 'spellbook' THEN 3
                ELSE 4
            END
            LIMIT 1
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (chain.lower(), address.lower()))
                row = cur.fetchone()
                return row[0] if row else None

    def count(self) -> int:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM protocol_contracts")
                return cur.fetchone()[0]

    def distinct_protocols(self) -> int:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(DISTINCT protocol) FROM protocol_contracts"
                )
                return cur.fetchone()[0]


# ----------------------------------------------------------------------
# Resolver factory
# ----------------------------------------------------------------------


def make_resolver(store: ProtocolContractStore) -> Callable[[str, str], str | None]:
    """Adapt a store to the (chain, address) -> protocol callable expected by
    `core.adapters.evm.registry.DecoderRegistry.protocol_resolver`."""
    return store.lookup
