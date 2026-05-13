"""Seed Spark Lend Pool address into protocol_contracts.

Spark Protocol is an Aave V3 fork by MakerDAO. Ethereum-only for the
main lending market.

Usage:
    PYTHONPATH=. python ops/seed_spark_contracts.py
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

logger = logging.getLogger("ops.seed_spark")

# Verified 2026-05-13 from https://docs.spark.fi/addresses.
SPARK_CONTRACTS = [
    ("ethereum", "0xc13e21b648a5ee794902342038ff3adab66be987"),  # SparkLend Pool
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
            protocol="spark",
            version=None,
            contract_type="protocol_contract",
            source="manual",
        )
        for chain, address in SPARK_CONTRACTS
    ]
    n = store.upsert_many(contracts)
    logger.info("upserted %d spark addresses", n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
