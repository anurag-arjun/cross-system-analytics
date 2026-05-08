"""Tests for the spellbook DEX SQL miner + YAML emitter."""

from __future__ import annotations

from pathlib import Path

from core.adapters.evm.spellbook.sql_miner import (
    BULK_PORT_FAMILIES,
    bulk_portable,
    deferred,
    summarise,
    walk_platforms,
    _parse_file,
)
from core.adapters.evm.spellbook.yaml_emitter import (
    deferred_manifest,
    emit_template_yamls,
)


_AERODROME_SQL = """
{{
    config(schema = 'aerodrome_base')
}}

WITH dexs_v1 AS (
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'base',
            project = 'aerodrome',
            version = '1',
            Pair_evt_Swap = source('aerodrome_base', 'Pool_evt_Swap'),
            Factory_evt_PairCreated = source('aerodrome_base', 'PoolFactory_evt_PoolCreated'),
            pair_column_name = 'pool'
        )
    }}
),
dexs_v2 AS (
    {{
        uniswap_compatible_v3_trades(
            blockchain = 'base',
            project = 'aerodrome',
            version = 'slipstream',
            Pair_evt_Swap = source('aerodrome_base', 'CLPool_evt_Swap'),
            Factory_evt_PoolCreated = source('aerodrome_base', 'CLFactory_evt_PoolCreated')
        )
    }}
)
SELECT 1
"""


def test_parse_file_extracts_two_invocations(tmp_path: Path) -> None:
    p = tmp_path / "aerodrome_base_base_trades.sql"
    p.write_text(_AERODROME_SQL)

    invs = _parse_file(p)

    assert len(invs) == 2
    families = {i.family for i in invs}
    assert families == {"uniswap_compatible_v2_trades", "uniswap_compatible_v3_trades"}
    versions = {i.version for i in invs}
    assert versions == {"1", "slipstream"}
    for inv in invs:
        assert inv.chain == "base"
        assert inv.project == "aerodrome"


def test_parse_file_ignores_unknown_macros(tmp_path: Path) -> None:
    p = tmp_path / "x.sql"
    p.write_text(
        "{{\n  some_unknown_macro(\n    blockchain = 'base',\n    project = 'foo'\n  )\n}}\n"
    )
    assert _parse_file(p) == []


def test_walk_platforms_traverses_in_scope_chains(tmp_path: Path) -> None:
    base = tmp_path / "dbt_subprojects" / "dex" / "models" / "trades"
    (base / "ethereum" / "platforms").mkdir(parents=True)
    (base / "polygon" / "platforms").mkdir(parents=True)
    (base / "bnb" / "platforms").mkdir(parents=True)  # out of scope

    (base / "ethereum" / "platforms" / "uni_eth.sql").write_text(
        _AERODROME_SQL.replace("'base'", "'ethereum'").replace("aerodrome", "uniswap")
    )
    (base / "polygon" / "platforms" / "qs_pol.sql").write_text(
        _AERODROME_SQL.replace("'base'", "'polygon'").replace("aerodrome", "quickswap")
    )
    (base / "bnb" / "platforms" / "ps_bnb.sql").write_text(
        _AERODROME_SQL.replace("'base'", "'bnb'").replace("aerodrome", "pancakeswap")
    )

    invs = walk_platforms(tmp_path, chains=("ethereum", "polygon"))
    chains = {i.chain for i in invs}
    assert chains == {"ethereum", "polygon"}
    assert all(i.chain != "bnb" for i in invs)


def test_emit_template_yamls(tmp_path: Path) -> None:
    from core.adapters.evm.spellbook.sql_miner import PlatformInvocation

    invs = [
        PlatformInvocation(
            chain="base",
            project="aerodrome",
            version="1",
            family="uniswap_compatible_v2_trades",
            source_file=tmp_path / "x.sql",
        ),
        PlatformInvocation(
            chain="ethereum",
            project="aerodrome",
            version="1",
            family="uniswap_compatible_v2_trades",
            source_file=tmp_path / "x.sql",
        ),
        PlatformInvocation(
            chain="base",
            project="aerodrome",
            version="slipstream",
            family="uniswap_compatible_v3_trades",
            source_file=tmp_path / "y.sql",
        ),
        # Deferred — should NOT emit a YAML.
        PlatformInvocation(
            chain="ethereum",
            project="curve",
            version="1",
            family="curve_compatible_v1_trades",
            source_file=tmp_path / "z.sql",
        ),
    ]

    out = tmp_path / "mappings"
    written = emit_template_yamls(invs, out)

    assert len(written) == 2
    v1 = (out / "aerodrome_v1.yaml").read_text()
    assert "protocol: aerodrome_v1" in v1
    assert "template: uniswap_v2" in v1
    # Both chains the project ships on
    assert "[base, ethereum]" in v1

    slip = (out / "aerodrome_vslipstream.yaml").read_text()
    assert "template: uniswap_v3" in slip

    # Deferred family — no YAML written
    assert not (out / "curve_v1.yaml").exists()


def test_emit_template_yamls_skips_existing(tmp_path: Path) -> None:
    from core.adapters.evm.spellbook.sql_miner import PlatformInvocation

    out = tmp_path / "mappings"
    out.mkdir()
    (out / "aerodrome_v1.yaml").write_text("hand-written\n")

    invs = [
        PlatformInvocation(
            chain="base",
            project="aerodrome",
            version="1",
            family="uniswap_compatible_v2_trades",
            source_file=tmp_path / "x.sql",
        ),
    ]
    written = emit_template_yamls(invs, out)
    assert written == []
    assert (out / "aerodrome_v1.yaml").read_text() == "hand-written\n"


def test_emit_template_yamls_does_not_overwrite_template_parent(tmp_path: Path) -> None:
    """If a project's slug equals its template slug (e.g. uniswap_v2 fork named
    'uniswap' version '2'), don't write — the parent owns that filename."""
    from core.adapters.evm.spellbook.sql_miner import PlatformInvocation

    invs = [
        PlatformInvocation(
            chain="ethereum",
            project="uniswap",
            version="2",
            family="uniswap_compatible_v2_trades",
            source_file=tmp_path / "x.sql",
        ),
    ]
    written = emit_template_yamls(invs, tmp_path / "mappings")
    assert written == []


def test_summarise_and_filter_helpers() -> None:
    from core.adapters.evm.spellbook.sql_miner import PlatformInvocation

    invs = [
        PlatformInvocation("base", "p1", "2", "uniswap_compatible_v2_trades", Path("x")),
        PlatformInvocation("base", "p2", "1", "curve_compatible_v1_trades", Path("y")),
    ]
    assert summarise(invs) == {
        "uniswap_compatible_v2_trades": 1,
        "curve_compatible_v1_trades": 1,
    }
    assert len(bulk_portable(invs)) == 1
    assert len(deferred(invs)) == 1


def test_deferred_manifest_groups_by_project() -> None:
    from core.adapters.evm.spellbook.sql_miner import PlatformInvocation

    invs = [
        PlatformInvocation("ethereum", "curve", "1", "curve_compatible_v1_trades", Path("x")),
        PlatformInvocation("polygon", "curve", "1", "curve_compatible_v1_trades", Path("y")),
        PlatformInvocation("base", "balancer", "2", "balancer_compatible_v2_trades", Path("z")),
    ]
    manifest = deferred_manifest(invs)
    assert len(manifest) == 2
    curve = next(m for m in manifest if m["project"] == "curve")
    assert curve["chains"] == ["ethereum", "polygon"]
    assert curve["family"] == "curve_compatible_v1_trades"


def test_bulk_port_template_set_is_consistent() -> None:
    """Sanity: every BULK_PORT_FAMILIES value is a known parent we ship."""
    parents = set(BULK_PORT_FAMILIES.values())
    repo_mappings = (
        Path(__file__).resolve().parents[2]
        / "adapters"
        / "evm"
        / "decoders"
        / "mappings"
    )
    on_disk = {p.stem for p in repo_mappings.glob("*.yaml")}
    missing = parents - on_disk
    assert not missing, f"BULK_PORT parents missing on disk: {missing}"
