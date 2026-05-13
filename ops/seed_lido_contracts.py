"""Seed Lido contract addresses into protocol_contracts.

Two contracts on Ethereum:
- stETH (Submitted event = stake)
- WithdrawalQueueERC721 (WithdrawalRequested event = unstake initiated)

Ethereum-only protocol — Lido's L2 wstETH bridges are out of scope here.

Usage:
    PYTHONPATH=. python ops/seed_lido_contracts.py
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

logger = logging.getLogger("ops.seed_lido")

# Verified 2026-05-13 from https://docs.lido.fi/deployed-contracts/.
LIDO_CONTRACTS = [
    ("ethereum", "0xae7ab96520de3a18e5e111b5eaab095312d7fe84"),  # stETH
    ("ethereum", "0x889edc2edab5f40e902b864ad4d7ade8e412f9b1"),  # WithdrawalQueueERC721
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
            protocol="lido",
            version=None,
            contract_type="protocol_contract",
            source="manual",
        )
        for chain, address in LIDO_CONTRACTS
    ]
    n = store.upsert_many(contracts)
    logger.info("upserted %d lido addresses", n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
