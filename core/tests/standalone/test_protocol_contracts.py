"""Tests for the protocol_contracts registry + spellbook importer.

The Postgres backend is exercised by integration tests elsewhere. Here we
cover the in-memory store and the importer logic against synthetic seed
files; an opt-in end-to-end test runs against a real spellbook clone if
one is present at the conventional sibling path.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from core.registry.protocol_contracts import (
    InMemoryProtocolContractStore,
    ProtocolContract,
    make_resolver,
)
from core.registry.spellbook_importer import (
    IN_SCOPE_CHAINS,
    iter_spellbook_contracts,
    import_spellbook,
)


# ---------------------------------------------------------------------------
# In-memory store
# ---------------------------------------------------------------------------


def test_inmemory_upsert_and_lookup():
    store = InMemoryProtocolContractStore()
    store.upsert_many(
        [
            ProtocolContract(
                chain="ethereum",
                address="0xAAaaaaaaaaaaaAAaaaaaaaaAAAAAAAAAAaAaaaaa",
                protocol="uniswap",
                version="3",
                source="spellbook",
            )
        ]
    )
    # Address + chain are case-insensitive via lookup
    assert store.lookup("ethereum", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") == "uniswap"
    assert store.lookup("ETHEREUM", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") == "uniswap"
    assert store.lookup("base", "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") is None
    assert store.count() == 1


def test_inmemory_source_priority():
    """When the same (chain, address) has multiple sources, manual wins
    over dune wins over spellbook."""
    store = InMemoryProtocolContractStore()
    addr = "0x" + "11" * 20
    store.upsert_many(
        [
            ProtocolContract(chain="base", address=addr, protocol="from_spellbook", source="spellbook"),
            ProtocolContract(chain="base", address=addr, protocol="from_dune", source="dune"),
        ]
    )
    assert store.lookup("base", addr) == "from_dune"
    store.upsert_many(
        [ProtocolContract(chain="base", address=addr, protocol="from_manual", source="manual")]
    )
    assert store.lookup("base", addr) == "from_manual"


def test_inmemory_normalizes_address_case():
    store = InMemoryProtocolContractStore()
    store.upsert_many(
        [
            ProtocolContract(
                chain="Base",
                address="0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
                protocol="aerodrome",
                source="spellbook",
            )
        ]
    )
    rows = store.all_rows()
    assert rows[0].chain == "base"
    assert rows[0].address == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def test_make_resolver_returns_callable():
    store = InMemoryProtocolContractStore()
    addr = "0x" + "cc" * 20
    store.upsert_many(
        [ProtocolContract(chain="base", address=addr, protocol="aerodrome", source="manual")]
    )
    resolver = make_resolver(store)
    assert resolver("base", addr) == "aerodrome"
    assert resolver("base", "0x" + "ff" * 20) is None


# ---------------------------------------------------------------------------
# Spellbook importer
# ---------------------------------------------------------------------------


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        f.write(",".join(header) + "\n")
        for row in rows:
            f.write(",".join(row) + "\n")


def test_importer_extracts_pool_seed(tmp_path: Path):
    seeds = tmp_path / "dbt_subprojects" / "dex" / "seeds" / "pools"
    _write_csv(
        seeds / "dex_pools_seed.csv",
        ["blockchain", "project", "version", "pool", "token_id", "token_address", "token_type"],
        [
            ["ethereum", "uniswap", "3", "0x" + "aa" * 20, "0", "0x" + "01" * 20, "pool_token"],
            ["base", "aerodrome", "1", "0x" + "bb" * 20, "0", "0x" + "02" * 20, "pool_token"],
            # Out-of-scope chain — should be dropped
            ["bnb", "pancakeswap", "2", "0x" + "cc" * 20, "0", "0x" + "03" * 20, "pool_token"],
        ],
    )

    records = list(iter_spellbook_contracts(tmp_path))
    by_address = {r.address: r for r in records}

    assert "0x" + "aa" * 20 in by_address
    assert "0x" + "bb" * 20 in by_address
    assert "0x" + "cc" * 20 not in by_address  # bnb dropped
    assert by_address["0x" + "aa" * 20].protocol == "uniswap"
    assert by_address["0x" + "aa" * 20].chain == "ethereum"
    assert by_address["0x" + "aa" * 20].source == "spellbook"
    assert by_address["0x" + "aa" * 20].contract_type == "pool"


def test_importer_dedupes_within_file(tmp_path: Path):
    seeds = tmp_path / "dbt_subprojects" / "dex" / "seeds" / "pools"
    addr = "0x" + "dd" * 20
    _write_csv(
        seeds / "dex_pools_seed.csv",
        ["blockchain", "project", "version", "pool", "token_id", "token_address", "token_type"],
        [
            ["ethereum", "curve", "2", addr, "0", "0x" + "01" * 20, "pool_token"],
            ["ethereum", "curve", "2", addr, "1", "0x" + "02" * 20, "pool_token"],
            ["ethereum", "curve", "2", addr, "2", "0x" + "03" * 20, "underlying_token_bought"],
        ],
    )
    records = list(iter_spellbook_contracts(tmp_path))
    assert len(records) == 1
    assert records[0].address == addr


def test_importer_handles_id_column_for_uniswap_liquidity(tmp_path: Path):
    seeds = tmp_path / "dbt_subprojects" / "dex" / "seeds" / "liquidity"
    _write_csv(
        seeds / "uniswap_ethereum_base_liquidity_seed.csv",
        [
            "blockchain", "project", "version", "event_type", "block_date",
            "tx_hash", "evt_index", "block_number", "id", "token0", "token1",
            "amount0_raw", "amount1_raw",
        ],
        [
            ["ethereum", "uniswap", "3",
             "modify_liquidity", "2025-01-01",
             "0x" + "ab" * 32, "0", "100",
             "0x" + "ee" * 20,
             "0x" + "01" * 20, "0x" + "02" * 20,
             "1", "1"],
        ],
    )
    records = list(iter_spellbook_contracts(tmp_path))
    assert len(records) == 1
    assert records[0].address == "0x" + "ee" * 20
    assert records[0].contract_type == "pool"


def test_importer_skips_non_address_id_values(tmp_path: Path):
    """UniV4 uses bytes32 pool IDs (64 hex chars). Importer must drop those
    rather than treat them as addresses."""
    seeds = tmp_path / "dbt_subprojects" / "dex" / "seeds" / "liquidity"
    _write_csv(
        seeds / "uniswap_polygon_base_liquidity_seed.csv",
        [
            "blockchain", "project", "version", "event_type", "block_date",
            "tx_hash", "evt_index", "block_number", "id", "token0", "token1",
            "amount0_raw", "amount1_raw",
        ],
        [
            # 64-char "id" — UniV4 pool id, not an address
            ["polygon", "uniswap", "4",
             "modify_liquidity", "2025-01-01",
             "0x" + "ab" * 32, "0", "100",
             "0x" + "5a" * 32,
             "0x" + "01" * 20, "0x" + "02" * 20,
             "1", "1"],
        ],
    )
    assert list(iter_spellbook_contracts(tmp_path)) == []


def test_importer_writes_to_store(tmp_path: Path):
    seeds = tmp_path / "dbt_subprojects" / "dex" / "seeds" / "pools"
    _write_csv(
        seeds / "dex_pools_seed.csv",
        ["blockchain", "project", "version", "pool", "token_id", "token_address", "token_type"],
        [
            ["arbitrum", "uniswap", "3", "0x" + "11" * 20, "0", "0x" + "01" * 20, "pool_token"],
            ["optimism", "velodrome", "2", "0x" + "22" * 20, "0", "0x" + "02" * 20, "pool_token"],
        ],
    )
    store = InMemoryProtocolContractStore()
    stats = import_spellbook(tmp_path, store)
    assert stats.rows_emitted == 2
    assert stats.distinct_addresses == 2
    assert stats.distinct_protocols == 2
    assert stats.chains == {"arbitrum", "optimism"}
    assert store.lookup("arbitrum", "0x" + "11" * 20) == "uniswap"
    assert store.lookup("optimism", "0x" + "22" * 20) == "velodrome"


def test_in_scope_chain_set_is_canonical():
    assert IN_SCOPE_CHAINS == frozenset(
        ["ethereum", "base", "arbitrum", "optimism", "polygon"]
    )


# ---------------------------------------------------------------------------
# End-to-end (only if the spellbook clone is present)
# ---------------------------------------------------------------------------


SPELLBOOK_PATH = Path(
    os.environ.get(
        "SPELLBOOK_PATH",
        "/home/lighto/code/avail-explorations/spellbook",
    )
)


@pytest.mark.skipif(
    not (SPELLBOOK_PATH / "dbt_subprojects").is_dir(),
    reason="No spellbook clone at SPELLBOOK_PATH",
)
def test_real_spellbook_extracts_some_known_protocols():
    store = InMemoryProtocolContractStore()
    stats = import_spellbook(SPELLBOOK_PATH, store)
    # We don't claim 10k rows from spellbook seeds — they're test fixtures.
    # We expect at least 50 rows, covering well-known protocols on at least
    # 3 in-scope chains.
    assert stats.rows_emitted >= 50, stats
    assert stats.distinct_protocols >= 5, stats
    protocols = {r.protocol for r in store.all_rows()}
    assert "uniswap" in protocols  # the most ubiquitous
    assert len(stats.chains & IN_SCOPE_CHAINS) >= 3


def test_lookup_slug_combines_protocol_and_version():
    """`lookup_slug` returns the YAML-mapping form `{protocol}_v{version}`."""
    store = InMemoryProtocolContractStore()
    addr1 = "0x" + "aa" * 20
    addr2 = "0x" + "bb" * 20
    addr3 = "0x" + "cc" * 20
    store.upsert_many(
        [
            ProtocolContract(chain="base", address=addr1, protocol="uniswap", version="3", source="dune"),
            ProtocolContract(chain="base", address=addr2, protocol="aerodrome", version="slipstream", source="dune"),
            ProtocolContract(chain="base", address=addr3, protocol="curve", source="manual"),
        ]
    )
    assert store.lookup_slug("base", addr1) == "uniswap_v3"
    assert store.lookup_slug("base", addr2) == "aerodrome_vslipstream"
    # No version -> bare protocol name
    assert store.lookup_slug("base", addr3) == "curve"
    # Missing address -> None
    assert store.lookup_slug("base", "0x" + "ff" * 20) is None


def test_make_resolver_returns_slug_form():
    """The decoder resolver must return the slug, not the bare protocol."""
    store = InMemoryProtocolContractStore()
    addr = "0x" + "11" * 20
    store.upsert_many(
        [ProtocolContract(chain="base", address=addr, protocol="uniswap", version="2", source="dune")]
    )
    resolver = make_resolver(store)
    assert resolver("base", addr) == "uniswap_v2"


def test_make_cached_resolver_serves_from_memory():
    """Cached resolver preloads all rows; subsequent lookups are O(1)
    and respect source priority."""
    from core.registry.protocol_contracts import make_cached_resolver

    store = InMemoryProtocolContractStore()
    addr1 = "0x" + "11" * 20
    addr2 = "0x" + "22" * 20
    addr3 = "0x" + "33" * 20
    store.upsert_many(
        [
            ProtocolContract(chain="base", address=addr1, protocol="aerodrome", version="1", source="dune"),
            ProtocolContract(chain="ethereum", address=addr2, protocol="curve", source="manual"),
            # Same (chain, address) with conflicting source — manual wins
            ProtocolContract(chain="optimism", address=addr3, protocol="velodrome", version="2", source="spellbook"),
            ProtocolContract(chain="optimism", address=addr3, protocol="velodrome", version="2", source="manual"),
        ]
    )
    resolver = make_cached_resolver(store)
    assert resolver("base", addr1) == "aerodrome_v1"
    assert resolver("ethereum", addr2) == "curve"
    assert resolver("optimism", addr3) == "velodrome_v2"
    assert resolver("base", "0x" + "ff" * 20) is None
