#!/usr/bin/env python3
"""CLI: walk protocol_contracts and ensure every (chain, address) has a
cached ABI in protocol_abis (keyed by deployed-bytecode hash for dedup).

Usage:
    PYTHONPATH=. python ops/fetch_abis.py [--limit N] [--chain ethereum] [--rate-limit 4]
    PYTHONPATH=. python ops/fetch_abis.py --postgres "$PROTOCOL_CONTRACTS_DSN" --verbose

Env:
    ETHERSCAN_API_KEY        Optional. Without it, only Sourcify is used.
    PROTOCOL_CONTRACTS_DSN   Optional Postgres DSN. Without it, runs in-memory
                             and reads contracts from the spellbook importer
                             output (dry-run mode is mostly useful as a smoke).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time

from dotenv import load_dotenv

from core.registry.abi_fetcher import ABIFetcher
from core.registry.abi_store import (
    InMemoryABIStore,
    InMemoryBytecodeStore,
    PostgresABIStore,
    PostgresBytecodeStore,
)
from core.registry.etherscan import EtherscanV2Client
from core.registry.protocol_contracts import (
    InMemoryProtocolContractStore,
    PostgresProtocolContractStore,
    ProtocolContract,
)
from core.registry.sourcify import SourcifyClient


def _read_contracts_postgres(dsn: str, chain: str | None, limit: int | None) -> list[tuple[str, str]]:
    import psycopg2

    sql = """
        SELECT DISTINCT chain, address
        FROM protocol_contracts
        WHERE address NOT IN (SELECT address FROM contract_bytecodes WHERE chain = protocol_contracts.chain)
    """
    args: list = []
    if chain:
        sql += " AND chain = %s"
        args.append(chain)
    sql += " ORDER BY chain, address"
    if limit:
        sql += " LIMIT %s"
        args.append(limit)

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            return [(row[0], row[1]) for row in cur.fetchall()]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--postgres",
        default=os.environ.get("PROTOCOL_CONTRACTS_DSN"),
        help="Postgres DSN. Without it, runs against an in-memory store seeded "
        "from the spellbook importer (mostly useful as a smoke test).",
    )
    parser.add_argument("--chain", help="Restrict to a single chain.")
    parser.add_argument("--limit", type=int, help="Max addresses to process.")
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=4.0,
        help="Etherscan calls per second (default 4; free-tier limit is 5).",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    etherscan_key = os.environ.get("ETHERSCAN_API_KEY")
    etherscan = (
        EtherscanV2Client(etherscan_key, rate_limit_rps=args.rate_limit)
        if etherscan_key
        else None
    )
    if etherscan is None:
        print("warning: ETHERSCAN_API_KEY unset — will only use Sourcify fallback")

    if args.postgres:
        items = _read_contracts_postgres(args.postgres, args.chain, args.limit)
        abi_store = PostgresABIStore(args.postgres)
        bc_store = PostgresBytecodeStore(args.postgres)
        mode = f"postgres ({args.postgres.split('@')[-1]})"
    else:
        # Smoke / dry-run: feed from the spellbook importer
        from pathlib import Path

        from core.registry.spellbook_importer import import_spellbook

        contracts = InMemoryProtocolContractStore()
        spellbook_path = Path(
            os.environ.get(
                "SPELLBOOK_PATH",
                "/home/lighto/code/avail-explorations/spellbook",
            )
        )
        if not (spellbook_path / "dbt_subprojects").is_dir():
            print(f"error: spellbook clone not at {spellbook_path}", file=sys.stderr)
            return 2
        import_spellbook(spellbook_path, contracts)
        items = [
            (r.chain, r.address)
            for r in contracts.all_rows()
            if (args.chain is None or r.chain == args.chain)
        ]
        if args.limit:
            items = items[: args.limit]
        abi_store = InMemoryABIStore()
        bc_store = InMemoryBytecodeStore()
        mode = "in-memory (dry-run from spellbook importer)"

    print(f"ABI fetcher -> {mode}")
    print(f"  addresses to process: {len(items)}")
    print(f"  rate limit: {args.rate_limit} req/s")
    print()

    t0 = time.time()
    with ABIFetcher(
        abi_store=abi_store,
        bytecode_store=bc_store,
        etherscan=etherscan,
        sourcify=SourcifyClient(),
    ) as fetcher:
        stats = fetcher.ensure_many(items)

    elapsed = time.time() - t0
    print()
    print(f"  addresses processed:  {stats.addresses_processed}")
    print(f"  ABIs fetched:         {stats.abis_fetched}")
    print(f"  cache hits:           {stats.cache_hits}")
    print(f"  proxies detected:     {stats.proxies_detected}")
    print(f"  not verified:         {stats.not_verified}")
    print(f"  sourcify hits:        {stats.sourcify_hits}")
    print(f"  errors:               {stats.errors}")
    print(f"  etherscan calls:      {stats.etherscan_calls}")
    print(f"  sourcify calls:       {stats.sourcify_calls}")
    print(f"  rpc calls:            {stats.rpc_calls}")
    if stats.bytecodes_fetched > 0 and stats.abis_fetched > 0:
        dedup = stats.bytecodes_fetched / stats.abis_fetched
        print(f"  dedup ratio:          {dedup:.2f}x (bytecodes per ABI)")
    print(f"  elapsed:              {elapsed:.1f}s")

    return 0


if __name__ == "__main__":
    sys.exit(main())
