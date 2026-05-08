#!/usr/bin/env python3
"""CLI: import contract addresses from a Dune Spellbook clone.

Usage:
    python ops/import_spellbook_contracts.py [--path PATH] [--dry-run]
    python ops/import_spellbook_contracts.py --postgres "postgresql://nexus:nexus@localhost:5434/nexus_ops"

Defaults:
    --path     /home/lighto/code/avail-explorations/spellbook  (or $SPELLBOOK_PATH)
    in-memory store (no DB write) unless --postgres is supplied
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from core.registry.protocol_contracts import (
    InMemoryProtocolContractStore,
    PostgresProtocolContractStore,
)
from core.registry.spellbook_importer import import_spellbook


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--path",
        default=os.environ.get(
            "SPELLBOOK_PATH",
            "/home/lighto/code/avail-explorations/spellbook",
        ),
        help="Path to a spellbook clone (default: $SPELLBOOK_PATH or sibling repo)",
    )
    parser.add_argument(
        "--postgres",
        default=os.environ.get("PROTOCOL_CONTRACTS_DSN"),
        help=(
            "Postgres DSN. If unset, runs in-memory and prints stats only "
            "(does not persist)."
        ),
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose log output",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(message)s",
    )

    spellbook_path = Path(args.path)
    if not (spellbook_path / "dbt_subprojects").is_dir():
        print(f"error: no spellbook clone at {spellbook_path}", file=sys.stderr)
        return 2

    if args.postgres:
        store = PostgresProtocolContractStore(args.postgres)
        mode = f"postgres ({args.postgres.split('@')[-1]})"
    else:
        store = InMemoryProtocolContractStore()
        mode = "in-memory (dry-run; pass --postgres to persist)"

    print(f"importing from {spellbook_path} -> {mode}")
    stats = import_spellbook(spellbook_path, store)

    print()
    print(f"  files scanned:        {stats.files_scanned}")
    print(f"  files with addresses: {stats.files_with_addresses}")
    print(f"  rows upserted:        {stats.rows_emitted}")
    print(f"  distinct addresses:   {stats.distinct_addresses}")
    print(f"  distinct protocols:   {stats.distinct_protocols}")
    print(f"  chains:               {sorted(stats.chains)}")

    if isinstance(store, PostgresProtocolContractStore):
        print(f"  total rows in db:     {store.count()}")
        print(f"  distinct protocols:   {store.distinct_protocols()}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
