"""Storage for the ABI cache + bytecode index.

Two tables, one indexed by content-addressed code_hash (the ABI cache itself)
and one indexed by (chain, address) (the bytecode index that resolves an
address to its code_hash + optional EIP-1967 implementation).

Both backends mirror the protocol_contracts pattern: in-memory for tests
and ad-hoc scripts, Postgres for production.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ABIRecord:
    code_hash: str
    abi_json: str
    source: str = "etherscan"


@dataclass(frozen=True)
class BytecodeRecord:
    chain: str
    address: str
    code_hash: str
    is_proxy: bool = False
    implementation_address: str | None = None

    def normalized(self) -> "BytecodeRecord":
        return BytecodeRecord(
            chain=self.chain.lower(),
            address=self.address.lower(),
            code_hash=self.code_hash.lower(),
            is_proxy=self.is_proxy,
            implementation_address=(
                self.implementation_address.lower() if self.implementation_address else None
            ),
        )


# ----------------------------------------------------------------------
# ABI store (keyed by code_hash)
# ----------------------------------------------------------------------


class ABIStore(ABC):
    @abstractmethod
    def upsert(self, record: ABIRecord) -> None: ...

    @abstractmethod
    def get(self, code_hash: str) -> str | None: ...

    @abstractmethod
    def has(self, code_hash: str) -> bool: ...

    @abstractmethod
    def count(self) -> int: ...


class InMemoryABIStore(ABIStore):
    def __init__(self) -> None:
        self._rows: dict[str, ABIRecord] = {}

    def upsert(self, record: ABIRecord) -> None:
        self._rows[record.code_hash.lower()] = record

    def get(self, code_hash: str) -> str | None:
        rec = self._rows.get(code_hash.lower())
        return rec.abi_json if rec else None

    def has(self, code_hash: str) -> bool:
        return code_hash.lower() in self._rows

    def count(self) -> int:
        return len(self._rows)


class PostgresABIStore(ABIStore):
    def __init__(self, dsn: str) -> None:
        import psycopg2  # noqa: F401

        self._dsn = dsn

    def _conn(self) -> Any:
        import psycopg2

        return psycopg2.connect(self._dsn)

    def upsert(self, record: ABIRecord) -> None:
        sql = """
            INSERT INTO protocol_abis (code_hash, abi_json, source)
            VALUES (%s, %s, %s)
            ON CONFLICT (code_hash) DO UPDATE SET
                abi_json = EXCLUDED.abi_json,
                source = EXCLUDED.source,
                fetched_at = now()
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (record.code_hash.lower(), record.abi_json, record.source))

    def get(self, code_hash: str) -> str | None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT abi_json FROM protocol_abis WHERE code_hash = %s",
                    (code_hash.lower(),),
                )
                row = cur.fetchone()
                return row[0] if row else None

    def has(self, code_hash: str) -> bool:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM protocol_abis WHERE code_hash = %s",
                    (code_hash.lower(),),
                )
                return cur.fetchone() is not None

    def count(self) -> int:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM protocol_abis")
                return cur.fetchone()[0]


# ----------------------------------------------------------------------
# Bytecode index (keyed by chain+address)
# ----------------------------------------------------------------------


class BytecodeStore(ABC):
    @abstractmethod
    def upsert(self, record: BytecodeRecord) -> None: ...

    @abstractmethod
    def get(self, chain: str, address: str) -> BytecodeRecord | None: ...

    @abstractmethod
    def count(self) -> int: ...


class InMemoryBytecodeStore(BytecodeStore):
    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], BytecodeRecord] = {}

    def upsert(self, record: BytecodeRecord) -> None:
        rn = record.normalized()
        self._rows[(rn.chain, rn.address)] = rn

    def get(self, chain: str, address: str) -> BytecodeRecord | None:
        return self._rows.get((chain.lower(), address.lower()))

    def count(self) -> int:
        return len(self._rows)


class PostgresBytecodeStore(BytecodeStore):
    def __init__(self, dsn: str) -> None:
        import psycopg2  # noqa: F401

        self._dsn = dsn

    def _conn(self) -> Any:
        import psycopg2

        return psycopg2.connect(self._dsn)

    def upsert(self, record: BytecodeRecord) -> None:
        rn = record.normalized()
        sql = """
            INSERT INTO contract_bytecodes
                (chain, address, code_hash, is_proxy, implementation_address)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (chain, address) DO UPDATE SET
                code_hash = EXCLUDED.code_hash,
                is_proxy = EXCLUDED.is_proxy,
                implementation_address = EXCLUDED.implementation_address,
                fetched_at = now()
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (rn.chain, rn.address, rn.code_hash, rn.is_proxy, rn.implementation_address),
                )

    def get(self, chain: str, address: str) -> BytecodeRecord | None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT chain, address, code_hash, is_proxy, implementation_address
                    FROM contract_bytecodes WHERE chain = %s AND address = %s
                    """,
                    (chain.lower(), address.lower()),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return BytecodeRecord(*row)

    def count(self) -> int:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM contract_bytecodes")
                return cur.fetchone()[0]
