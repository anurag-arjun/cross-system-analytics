"""Seed Circle CCTP V1 TokenMessenger addresses into protocol_contracts.

Each chain has its own TokenMessenger; the V1 contracts are pinned per
Circle's deployed-contracts page. CCTP V2 deployments use different
addresses and aren't seeded here.

Usage:
    PYTHONPATH=. python ops/seed_cctp_contracts.py
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

logger = logging.getLogger("ops.seed_cctp")

# CCTP V1 TokenMessenger contracts. Verified 2026-05-13 from
# https://developers.circle.com/stablecoins/docs/evm-smart-contracts.
CCTP_V1_TOKEN_MESSENGERS = [
    ("ethereum", "0xbd3fa81b58ba92a82136038b25adec7066af3155"),
    ("base",     "0x1682ae6375c4e4a97e4b583bc394c861a46d8962"),
    ("arbitrum", "0x19330d10d9cc8751218eaf51e8885d058642e08a"),
    ("optimism", "0x2b4069517957735be00cee0fadae88a26365528f"),
    ("polygon",  "0x9daf8c91aefae50b9c0e69629d3f6ca40ca3b3fe"),
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
    # version=None -> slug "cctp" (matches the YAML's protocol: field).
    # Setting version="1" would produce slug "cctp_v1" with no matching
    # decoder; the address-first lookup would silently fall back to topic0
    # alone. CCTP V2 contracts can be seeded as protocol="cctp_v2" later.
    contracts = [
        ProtocolContract(
            chain=chain,
            address=address,
            protocol="cctp",
            version=None,
            contract_type="bridge",
            source="manual",
        )
        for chain, address in CCTP_V1_TOKEN_MESSENGERS
    ]
    n = store.upsert_many(contracts)
    logger.info("upserted %d cctp TokenMessenger addresses", n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
