"""Seed Morpho Blue singleton addresses into protocol_contracts.

Same singleton address on every chain it's deployed to. Ethereum + Base
are the primary BD-scope deployments.

Usage:
    PYTHONPATH=. python ops/seed_morpho_blue_contracts.py
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv  # noqa: E402

from core.registry.protocol_contracts import (  # noqa: E402
    PostgresProtocolContractStore,
    ProtocolContract,
)

load_dotenv()

logger = logging.getLogger("ops.seed_morpho_blue")

# Verified 2026-05-13 from https://docs.morpho.org/morpho/contracts/.
MORPHO_BLUE_SINGLETON = "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb"

MORPHO_BLUE_CONTRACTS = [
    ("ethereum", MORPHO_BLUE_SINGLETON),
    ("base",     MORPHO_BLUE_SINGLETON),
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--postgres",
        default=os.environ.get(
            "PROTOCOL_CONTRACTS_DSN",
            "postgresql://nexus:nexus@localhost:5434/nexus_ops",
        ),
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    store = PostgresProtocolContractStore(args.postgres)
    contracts = [
        ProtocolContract(
            chain=chain,
            address=address,
            protocol="morpho_blue",
            version=None,
            contract_type="protocol_contract",
            source="manual",
        )
        for chain, address in MORPHO_BLUE_CONTRACTS
    ]
    n = store.upsert_many(contracts)
    logger.info("upserted %d morpho_blue addresses", n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
