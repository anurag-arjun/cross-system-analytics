"""Seed Aave V3 main-market Pool addresses into protocol_contracts.

These are the canonical Pool contracts for Aave V3 on each BD-scope chain.
With source='manual' they outrank any Dune/Spellbook labels for the same
address (lookup priority: manual > dune > spellbook), so the decoder
registry always routes Supply/Withdraw/Borrow/Repay logs at these
addresses to the aave_v3 YAML mapping.

Idempotent — `(chain, address, source)` is the composite PK.

Usage:
    PYTHONPATH=. python ops/seed_aave_v3_contracts.py
    PYTHONPATH=. python ops/seed_aave_v3_contracts.py --postgres "postgresql://..."
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

logger = logging.getLogger("ops.seed_aave_v3")


# Aave V3 main-market Pool addresses. Verified 2026-05-13 from
# https://aave.com/docs/resources/addresses. Other Aave markets (Lido,
# EtherFi, etc.) are out of scope for this seed — add separately as the
# need arises so we can keep them under distinct `version` strings.
AAVE_V3_POOLS = [
    ("ethereum", "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2"),
    ("base",     "0xa238dd80c259a72e81d7e4664a9801593f98d1c5"),
    ("arbitrum", "0x794a61358d6845594f94dc1db02a252b5b4814ad"),
    ("optimism", "0x794a61358d6845594f94dc1db02a252b5b4814ad"),
    ("polygon",  "0x794a61358d6845594f94dc1db02a252b5b4814ad"),
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
    # protocol="aave_v3", version=None -> slug "aave_v3" (matches the YAML
    # mapping at core/adapters/evm/decoders/mappings/aave_v3.yaml).
    # contract_type="protocol_contract" keeps the resolver priority bucket
    # generic — the dex/aggregator/bridge buckets exist for Dune-bootstrap
    # disambiguation, which doesn't apply to a manually-seeded lending pool.
    contracts = [
        ProtocolContract(
            chain=chain,
            address=address,
            protocol="aave_v3",
            version=None,
            contract_type="protocol_contract",
            source="manual",
        )
        for chain, address in AAVE_V3_POOLS
    ]
    n = store.upsert_many(contracts)
    logger.info("upserted %d aave_v3 Pool addresses", n)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
