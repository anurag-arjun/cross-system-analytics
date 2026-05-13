"""Seed Compound V3 (Comet) addresses into protocol_contracts.

Each Comet is a single-base-asset lending market. Mainnet markets cover
USDC, WETH, USDT on the major chains; some chains also have bridged
USDC variants.

Verified 2026-05-13 from https://docs.compound.finance/#networks.

Usage:
    PYTHONPATH=. python ops/seed_compound_v3_contracts.py
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

logger = logging.getLogger("ops.seed_compound_v3")

COMPOUND_V3_COMETS = [
    # Ethereum
    ("ethereum", "0xc3d688b66703497daa19211eedff47f25384cdc3"),  # cUSDCv3
    ("ethereum", "0xa17581a9e3356d9a858b789d68b4d866e593ae94"),  # cWETHv3
    ("ethereum", "0x3afdc9bca9213a35503b077a6072f3d0d5ab0840"),  # cUSDTv3
    # Base
    ("base",     "0xb125e6687d4313864e53df431d5425969c15eb2f"),  # cUSDCv3
    ("base",     "0x46e6b214b524310239732d51387075e0e70970bf"),  # cWETHv3
    ("base",     "0x9c4ec768c28520b50860ea7a15bd7213a9ff58bf"),  # cUSDbCv3 (bridged)
    # Arbitrum
    ("arbitrum", "0x9c4ec768c28520b50860ea7a15bd7213a9ff58bf"),  # cUSDCv3 (native)
    ("arbitrum", "0xa5edbdd9646f8dff606d7448e414884c7d905dca"),  # cUSDC.ev3 (bridged)
    ("arbitrum", "0x6f7d514bbd4aff3bcd1140b7344b32f063dee486"),  # cWETHv3
    ("arbitrum", "0xd98be00b5d27fc98112bde293e487f8d4ca57d07"),  # cUSDTv3
    # Optimism
    ("optimism", "0x2e44e174f7d53f0212823acc11c01a11d58c5bcb"),  # cUSDCv3
    ("optimism", "0xe36a30d249f7761327fd973001a32010b521b6fd"),  # cWETHv3
    ("optimism", "0x995e394b8b2437ac8ce61ee0bc610d617962b214"),  # cUSDTv3
    # Polygon
    ("polygon",  "0xf25212e676d1f7f89cd72ffee66158f541246445"),  # cUSDCv3
    ("polygon",  "0xaeb318360f27748acb200ce616e389a6c9409a07"),  # cUSDTv3
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
            protocol="compound_v3",
            version=None,
            contract_type="protocol_contract",
            source="manual",
        )
        for chain, address in COMPOUND_V3_COMETS
    ]
    n = store.upsert_many(contracts)
    logger.info("upserted %d compound_v3 Comet addresses", n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
