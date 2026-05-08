"""Storage for human-readable contract labels.

Mirrors `protocol_contracts.py`: in-memory + Postgres backends, source-keyed
composite primary key so dune/manual labels coexist for the same address.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ContractLabel:
    chain: str
    address: str
    label: str
    category: str | None = None
    source: str = "manual"

    def normalized(self) -> "ContractLabel":
        return ContractLabel(
            chain=self.chain.lower(),
            address=self.address.lower(),
            label=self.label,
            category=self.category,
            source=self.source,
        )


class ContractLabelStore(ABC):
    @abstractmethod
    def upsert_many(self, labels: Iterable[ContractLabel]) -> int:
        ...

    @abstractmethod
    def lookup(self, chain: str, address: str) -> str | None:
        ...

    @abstractmethod
    def count(self) -> int:
        ...


class InMemoryContractLabelStore(ContractLabelStore):
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str, str], ContractLabel] = {}

    def upsert_many(self, labels: Iterable[ContractLabel]) -> int:
        n = 0
        for la in labels:
            ln = la.normalized()
            self._rows[(ln.chain, ln.address, ln.source)] = ln
            n += 1
        return n

    def lookup(self, chain: str, address: str) -> str | None:
        chain = chain.lower()
        address = address.lower()
        for src in ("manual", "dune"):
            row = self._rows.get((chain, address, src))
            if row is not None:
                return row.label
        return None

    def count(self) -> int:
        return len(self._rows)

    def all_rows(self) -> list[ContractLabel]:
        return list(self._rows.values())


class PostgresContractLabelStore(ContractLabelStore):
    def __init__(self, dsn: str) -> None:
        import psycopg2  # noqa: F401

        self._dsn = dsn

    def _conn(self) -> Any:
        import psycopg2

        return psycopg2.connect(self._dsn)

    def upsert_many(self, labels: Iterable[ContractLabel]) -> int:
        rows = [
            (la.normalized().chain, la.normalized().address, la.label, la.category, la.source)
            for la in labels
        ]
        if not rows:
            return 0
        sql = """
            INSERT INTO contract_labels
                (chain, address, label, category, source)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (chain, address, source) DO UPDATE SET
                label = EXCLUDED.label,
                category = EXCLUDED.category,
                fetched_at = now()
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        return len(rows)

    def lookup(self, chain: str, address: str) -> str | None:
        sql = """
            SELECT label FROM contract_labels
            WHERE chain = %s AND address = %s
            ORDER BY CASE source
                WHEN 'manual' THEN 1
                WHEN 'dune'   THEN 2
                ELSE 3
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
                cur.execute("SELECT COUNT(*) FROM contract_labels")
                return cur.fetchone()[0]
