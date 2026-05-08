"""Persistent UniV4 pool registry.

UniV4 routes every swap through a singleton PoolManager. Swap events emit
PoolId (bytes32 hash of the PoolKey) but no token addresses — those live
in the separate Initialize event, which fires once per pool. This module
stores pool metadata (currency0, currency1, fee, tickSpacing, hooks) so
the swap decoder can resolve tokens from PoolId.

The registry is small (one row per pool ever created), so production
lookups go through `make_cached_pool_resolver` which preloads the entire
table into a dict on first use. Stale-cache risk is low: pools created
mid-ingestion are missed for one run and picked up on the next reload.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable

from .protocol_contracts import _slug  # noqa: F401  (kept for parity)


@dataclass(frozen=True)
class UniV4Pool:
    chain: str
    pool_id: str
    pool_manager: str
    currency0: str
    currency1: str
    fee: int
    tick_spacing: int
    hooks: str
    init_block: int
    init_block_time: datetime
    init_tx_hash: str


class UniV4PoolStore:
    """Postgres-backed UniV4 pool registry. Lives in `nexus_ops.uniswap_v4_pools`."""

    def __init__(self, dsn: str) -> None:
        import psycopg2  # noqa: F401  (import-time dependency check)

        self._dsn = dsn

    def _conn(self) -> Any:
        import psycopg2

        return psycopg2.connect(self._dsn)

    def upsert_many(self, pools: Iterable[UniV4Pool]) -> int:
        rows = [
            (
                p.chain.lower(),
                p.pool_id.lower(),
                p.pool_manager.lower(),
                p.currency0.lower(),
                p.currency1.lower(),
                p.fee,
                p.tick_spacing,
                p.hooks.lower(),
                p.init_block,
                p.init_block_time,
                p.init_tx_hash.lower(),
            )
            for p in pools
        ]
        if not rows:
            return 0
        sql = """
            INSERT INTO uniswap_v4_pools (
                chain, pool_id, pool_manager, currency0, currency1,
                fee, tick_spacing, hooks,
                init_block, init_block_time, init_tx_hash
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (chain, pool_id) DO NOTHING
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
        return len(rows)

    def all_rows(self) -> list[UniV4Pool]:
        sql = """
            SELECT chain, pool_id, pool_manager, currency0, currency1,
                   fee, tick_spacing, hooks,
                   init_block, init_block_time, init_tx_hash
            FROM uniswap_v4_pools
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return [UniV4Pool(*r) for r in rows]

    def count(self) -> int:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM uniswap_v4_pools")
                return cur.fetchone()[0]


PoolResolver = Callable[[str, str], "tuple[str, str] | None"]
"""(chain, pool_id) -> (currency0, currency1) or None."""


def make_cached_pool_resolver(store: UniV4PoolStore) -> PoolResolver:
    """Preload all pools into a dict and return a (chain, pool_id) -> tokens lookup."""
    by_key: dict[tuple[str, str], tuple[str, str]] = {}
    for p in store.all_rows():
        by_key[(p.chain.lower(), p.pool_id.lower())] = (p.currency0, p.currency1)

    def _resolve(chain: str, pool_id: str) -> tuple[str, str] | None:
        return by_key.get((chain.lower(), pool_id.lower()))

    return _resolve
