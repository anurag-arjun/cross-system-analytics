#!/usr/bin/env python3
"""CLI: bootstrap the protocol_contracts + contract_labels registries from Dune.

Usage:
    PYTHONPATH=. python ops/import_dune_contracts.py [--days 90] [--byte-cap-mb 50] [--skip-labels]
    PYTHONPATH=. python ops/import_dune_contracts.py --postgres "$PROTOCOL_CONTRACTS_DSN"

Env:
    DUNE_API_KEY                Required.
    PROTOCOL_CONTRACTS_DSN      Optional Postgres DSN. If unset, runs in-memory.

Defaults:
    --days        90
    --byte-cap-mb 50
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from dotenv import load_dotenv

from core.registry.contract_labels import (
    InMemoryContractLabelStore,
    PostgresContractLabelStore,
)
from core.registry.dune import DuneClient
from core.registry.dune_bootstrap import (
    DEFAULT_BYTE_CAP,
    IN_SCOPE_CHAINS,
    run_bootstrap,
)
from core.registry.protocol_contracts import (
    InMemoryProtocolContractStore,
    PostgresProtocolContractStore,
)

# Load .env at module-import time so argparse defaults that read os.environ
# (e.g. PROTOCOL_CONTRACTS_DSN below) actually see the .env values. The
# previous placement was inside main() AFTER add_argument, which silently
# left those defaults at None and forced callers to pass --postgres.
load_dotenv()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument(
        "--byte-cap-mb",
        type=float,
        default=DEFAULT_BYTE_CAP / (1024 * 1024),
        help="Hard cap on cumulative bytes downloaded across all phases.",
    )
    parser.add_argument(
        "--postgres",
        default=os.environ.get("PROTOCOL_CONTRACTS_DSN"),
        help="Postgres DSN. If unset, runs in-memory and prints stats only.",
    )
    parser.add_argument(
        "--skip-labels",
        action="store_true",
        help="Skip the labels.addresses JOIN phase.",
    )
    parser.add_argument(
        "--chains",
        nargs="+",
        default=list(IN_SCOPE_CHAINS),
        help="Chain slugs to include.",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    api_key = os.environ.get("DUNE_API_KEY")
    if not api_key:
        print("error: DUNE_API_KEY not set in env or .env", file=sys.stderr)
        return 2

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if args.postgres:
        contract_store = PostgresProtocolContractStore(args.postgres)
        label_store = PostgresContractLabelStore(args.postgres)
        mode = f"postgres ({args.postgres.split('@')[-1]})"
    else:
        contract_store = InMemoryProtocolContractStore()
        label_store = InMemoryContractLabelStore()
        mode = "in-memory (dry-run)"

    print(f"Dune bootstrap -> {mode}")
    print(f"  chains: {args.chains}")
    print(f"  days:   {args.days}")
    print(f"  byte cap: {args.byte_cap_mb:.1f} MB")
    print()

    byte_cap = int(args.byte_cap_mb * 1024 * 1024)

    with DuneClient(api_key=api_key) as client:
        stats = run_bootstrap(
            client,
            contract_store,
            label_store,
            chains=tuple(args.chains),
            days=args.days,
            byte_cap=byte_cap,
            skip_labels=args.skip_labels,
        )

    print()
    print(f"  contracts upserted:   {stats.contracts_upserted}")
    print(f"  labels upserted:      {stats.labels_upserted}")
    print(f"  distinct protocols:   {len(stats.distinct_protocols)}")
    print(f"  distinct chains:      {sorted(stats.distinct_chains)}")
    print(f"  queries run:          {stats.budget.queries}")
    print(f"  total bytes:          {stats.budget.total_bytes:,}")
    print(f"  total exec ms:        {stats.budget.total_execution_ms:,}")
    print(
        f"  estimated credits:    "
        f"{stats.budget.estimate_credits():.1f} (free 20/MB) | "
        f"{stats.budget.estimate_credits(2):.1f} (plus 2/MB)"
    )
    if stats.aborted_at:
        print(f"  aborted at phase:     {stats.aborted_at}")
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
