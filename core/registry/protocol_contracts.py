"""Storage for the (chain, address) -> protocol registry.

Two backends:

- ``InMemoryProtocolContractStore`` — a dict for tests, ad-hoc scripts, and
  small fixture sets.
- ``PostgresProtocolContractStore`` — production. Reads/writes
  ``protocol_contracts`` (see core/schemas/postgres/protocol_contracts.sql).

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
    def lookup_slug(self, chain: str, address: str) -> str | None:
        """Return ``{protocol}_v{version}`` for (chain, address), or None.

        This is the form decoder YAML mappings use: ``aerodrome_v1``,
        ``uniswap_v2``, ``uniswap_v3``, etc. When ``version`` is empty/None
        the slug is just the bare protocol name.
        """

    @abstractmethod
    def count(self) -> int:
        """Total rows in the store."""


def _slug(protocol: str | None, version: str | None) -> str | None:
    if not protocol:
        return None
    if not version:
        return protocol
    return f"{protocol}_v{version}"


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

    def _resolve(self, chain: str, address: str) -> ProtocolContract | None:
        chain = chain.lower()
        address = address.lower()
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
                    return c
        return candidates[0]

    def lookup(self, chain: str, address: str) -> str | None:
        c = self._resolve(chain, address)
        return c.protocol if c else None

    def lookup_slug(self, chain: str, address: str) -> str | None:
        c = self._resolve(chain, address)
        return _slug(c.protocol, c.version) if c else None

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
        return self._fetch_one(chain, address, columns="protocol", to_slug=False)

    def lookup_slug(self, chain: str, address: str) -> str | None:
        return self._fetch_one(chain, address, columns="protocol, version", to_slug=True)

    def _fetch_one(self, chain: str, address: str, columns: str, to_slug: bool) -> str | None:
        sql = f"""
            SELECT {columns} FROM protocol_contracts
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
                if not row:
                    return None
                if to_slug:
                    protocol, version = row[0], row[1]
                    return _slug(protocol, version)
                return row[0]

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

    def all_rows(self) -> list[ProtocolContract]:
        """Bulk-fetch the entire registry. Used by `make_cached_resolver`."""
        sql = """
            SELECT chain, address, protocol, version, contract_type, source
            FROM protocol_contracts
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return [
            ProtocolContract(
                chain=r[0],
                address=r[1],
                protocol=r[2],
                version=r[3],
                contract_type=r[4],
                source=r[5],
            )
            for r in rows
        ]


# ----------------------------------------------------------------------
# Resolver factory
# ----------------------------------------------------------------------


def make_resolver(store: ProtocolContractStore) -> Callable[[str, str], str | None]:
    """Adapt a store to the (chain, address) -> slug callable expected by
    `core.adapters.evm.registry.DecoderRegistry.protocol_resolver`.

    The slug form ``{protocol}_v{version}`` matches the YAML mapping
    namespace, so e.g. an Aerodrome v1 pool resolves to ``aerodrome_v1``
    and the registry finds the corresponding decoder. For legacy callers
    that need the bare protocol, use ``store.lookup`` directly.
    """
    return store.lookup_slug


def make_cached_resolver(
    store: ProtocolContractStore,
) -> Callable[[str, str], str | None]:
    """Preload the entire registry into memory and return a slug resolver.

    Decoder lookups happen once per ingested log (millions per Dagster run),
    so a per-call Postgres roundtrip is fatal. The full ``protocol_contracts``
    table is small (~30k rows after a 1-day Dune bootstrap), so we load it
    into a dict at construction. This trades a one-shot ~50ms preload for
    O(1) memory lookups thereafter. Refresh by rebuilding the resolver — the
    underlying store is the source of truth.

    When the same (chain, address) appears under multiple labels (Dune
    publishes the same pool both as e.g. `aerodrome` in `dex.trades` and
    as a `bitget_dex_aggregator` row in `dex_aggregator.trades`), the
    resolver prefers the underlying-DEX label over the aggregator label.
    Without this rule, aggregator-tagged pools would resolve to a slug
    with no YAML mapping, mis-attributing the swap to the topic0
    fallback decoder (typically uniswap_v2).
    """
    rows = list(store.all_rows())
    # Compound priority (lower wins):
    #   1. is_manual — operator override always wins.
    #   2. contract_type — dex > protocol_contract > bridge > aggregator.
    #      A pool labelled both "aerodrome" (dex) and "bitget_dex_aggregator"
    #      (aggregator) resolves as aerodrome — the aggregator label has no
    #      YAML decoder anyway, so without this rule the topic0 fallback
    #      mis-attributed the swap to uniswap_v2.
    #   3. source — manual > dune > spellbook.
    src_priority = {"manual": 0, "dune": 1, "spellbook": 2}
    type_priority = {
        "dex": 0,
        "protocol_contract": 1,
        "bridge": 2,
        "aggregator": 3,
    }

    def _key(r) -> tuple[int, int, int]:
        is_manual = 0 if r.source == "manual" else 1
        type_p = type_priority.get(r.contract_type, 1)  # NULL → middle
        src_p = src_priority.get(r.source, 99)
        return (is_manual, type_p, src_p)

    rows.sort(key=_key)
    by_addr: dict[tuple[str, str], str] = {}
    for r in rows:
        key = (r.chain.lower(), r.address.lower())
        if key in by_addr:
            continue  # higher-priority candidate already won
        by_addr[key] = _slug(r.protocol, r.version) or r.protocol

    def _resolve(chain: str, address: str) -> str | None:
        return by_addr.get((chain.lower(), address.lower()))

    return _resolve
