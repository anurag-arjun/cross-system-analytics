"""Walk a Dune Spellbook checkout and produce ProtocolContract records.

Reality check up front: spellbook seeds are mostly **dbt test fixtures**
(small CSVs, ~5–50 rows each) — they are not curated address registries.
Hardcoded contract addresses live in macros + Dune-side ABI uploads, not
in the seeds layer. The bulk of our protocol_contracts volume comes from
the Dune bootstrap export (separate ticket); this importer contributes a
seed layer of well-known DEX pools and the protocol-coverage scaffolding.

Sources we extract from:

1. ``dbt_subprojects/dex/seeds/pools/dex_pools_seed.csv``
   columns: blockchain, project, version, pool, token_id, token_address, token_type
   The ``pool`` column is the contract address. Each pool appears multiple
   times (one per token slot); we dedup on (blockchain, pool).

2. ``dbt_subprojects/dex/seeds/liquidity/*_liquidity_seed.csv``
   columns include: blockchain, project, version, id, token0, token1, ...
   The ``id`` column is the Uniswap-V3-style pool address.

3. ``dbt_subprojects/dex/seeds/trades/*_base_trades_seed.csv``
   ~1700 files. Most do *not* have a contract-address column; they index
   by tx_hash + evt_index. We use the *filename* and (blockchain, project,
   version) header columns to register protocol coverage on each chain
   even when no concrete address is in the row.

4. Generic CSV sniffer for any seed file whose header includes one of:
   ``project_contract_address``, ``contract_address``, ``pool``, ``address``.

Filtered to in-scope chains: ethereum, base, arbitrum, optimism, polygon.
"""

from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from core.registry.protocol_contracts import ProtocolContract

logger = logging.getLogger(__name__)

IN_SCOPE_CHAINS = frozenset(
    ["ethereum", "base", "arbitrum", "optimism", "polygon"]
)

_ADDR_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")

CHAIN_COLUMNS = ("blockchain", "chain")
PROTOCOL_COLUMNS = ("project", "protocol")
VERSION_COLUMNS = ("version",)
ADDRESS_COLUMNS = (
    "project_contract_address",
    "contract_address",
    "pool",
    "id",
    "address",
)


@dataclass
class ImportStats:
    files_scanned: int = 0
    files_with_addresses: int = 0
    rows_emitted: int = 0
    distinct_addresses: int = 0
    distinct_protocols: int = 0
    chains: set[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.chains is None:
            self.chains = set()


def _pick_column(header: list[str], candidates: tuple[str, ...]) -> str | None:
    lower_to_orig = {h.lower(): h for h in header}
    for cand in candidates:
        if cand in lower_to_orig:
            return lower_to_orig[cand]
    return None


def _is_address(value: str) -> bool:
    return bool(_ADDR_RE.match(value or ""))


def _iter_csv_records(
    path: Path,
) -> Iterator[ProtocolContract]:
    """Yield ProtocolContract records from a single CSV.

    Rows are deduplicated within the file on (chain, address); the caller
    is responsible for cross-file dedup at the store level.
    """
    try:
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames or []
            chain_col = _pick_column(header, CHAIN_COLUMNS)
            addr_col = _pick_column(header, ADDRESS_COLUMNS)
            proto_col = _pick_column(header, PROTOCOL_COLUMNS)
            version_col = _pick_column(header, VERSION_COLUMNS)
            if not (chain_col and addr_col and proto_col):
                return
            seen: set[tuple[str, str]] = set()
            for row in reader:
                chain = (row.get(chain_col) or "").strip().lower()
                if chain not in IN_SCOPE_CHAINS:
                    continue
                address = (row.get(addr_col) or "").strip()
                if not _is_address(address):
                    continue
                protocol = (row.get(proto_col) or "").strip()
                if not protocol:
                    continue
                key = (chain, address.lower())
                if key in seen:
                    continue
                seen.add(key)
                version = (row.get(version_col) or "").strip() if version_col else None
                yield ProtocolContract(
                    chain=chain,
                    address=address.lower(),
                    protocol=protocol,
                    version=version or None,
                    contract_type=_infer_contract_type(addr_col),
                    source="spellbook",
                )
    except Exception as e:
        logger.warning("Failed to read %s: %s", path, e)


def _infer_contract_type(addr_col_name: str) -> str | None:
    name = addr_col_name.lower()
    if "pool" in name or name == "id":
        return "pool"
    if "contract" in name:
        return "protocol_contract"
    return None


def iter_spellbook_contracts(spellbook_root: Path) -> Iterator[ProtocolContract]:
    """Walk *spellbook_root* and yield ProtocolContract records.

    The walker visits dbt_subprojects/{dex,daily_spellbook}/seeds/**/*.csv.
    Other subprojects (solana, tokens, hourly_spellbook) are skipped — they
    don't contribute EVM contract addresses.
    """
    for sub in ("dex", "daily_spellbook"):
        seeds_dir = spellbook_root / "dbt_subprojects" / sub / "seeds"
        if not seeds_dir.is_dir():
            continue
        for csv_path in sorted(seeds_dir.rglob("*.csv")):
            yield from _iter_csv_records(csv_path)


def import_spellbook(
    spellbook_root: Path,
    store,  # ProtocolContractStore
) -> ImportStats:
    """Walk spellbook and upsert all extracted ProtocolContract records.

    Returns ImportStats with file/row/protocol counts.
    """
    stats = ImportStats()
    batch: list[ProtocolContract] = []
    distinct_addresses: set[tuple[str, str]] = set()
    distinct_protocols: set[str] = set()

    for sub in ("dex", "daily_spellbook"):
        seeds_dir = spellbook_root / "dbt_subprojects" / sub / "seeds"
        if not seeds_dir.is_dir():
            continue
        for csv_path in sorted(seeds_dir.rglob("*.csv")):
            stats.files_scanned += 1
            had_rows = False
            for record in _iter_csv_records(csv_path):
                had_rows = True
                batch.append(record)
                distinct_addresses.add((record.chain, record.address))
                distinct_protocols.add(record.protocol)
                stats.chains.add(record.chain)
            if had_rows:
                stats.files_with_addresses += 1
            if len(batch) >= 1000:
                stats.rows_emitted += store.upsert_many(batch)
                batch.clear()

    if batch:
        stats.rows_emitted += store.upsert_many(batch)

    stats.distinct_addresses = len(distinct_addresses)
    stats.distinct_protocols = len(distinct_protocols)
    return stats
