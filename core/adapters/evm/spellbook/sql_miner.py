"""Mine spellbook DEX trades SQL files for (chain, project, version, family) tuples.

Spellbook's DEX subproject organises platform decoders as
`dbt_subprojects/dex/models/trades/<chain>/platforms/<project>_<chain>_base_trades.sql`.

Each file calls one or more dbt macros that encapsulate a fork family:

* `uniswap_compatible_v2_trades` — UniV2 fork (Aerodrome v1, BaseSwap, etc.).
* `uniswap_compatible_v3_trades` — UniV3 fork (Aerodrome slipstream, SushiSwap V3, …).
* `uniswap_compatible_v4_trades` — UniV4 (singleton PoolManager).
* `balancer_compatible_v[123]_trades` — Balancer (vault-based).
* `curve_compatible_*_trades` — Curve (pool-registry-aware).
* `maverick_compatible_*` — Maverick AMMs.
* `gmx_compatible_*` — GMX-family perps.
* …and many bespoke families.

We extract `(blockchain, project, version, family)` rows. Downstream callers
emit YAML mappings — bulk-portable for v2/v3-compatible families, hand-curated
for the rest.

Important context: spellbook macros consume `Pair_evt_Swap = source(...)` —
i.e. Dune's already-decoded event tables. They never declare topic0 hex
literals. The miner's purpose is therefore to enumerate the fork *taxonomy*,
not to extract on-chain event hashes; topic0s come from our own
`mappings/uniswap_v[234].yaml` event signatures.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

# Macro families we recognise. Add to this list as new fork families surface.
# Order matters only for documentation; matching is exact.
KNOWN_FAMILIES: tuple[str, ...] = (
    "uniswap_compatible_v2_trades",
    "uniswap_compatible_v3_trades",
    "uniswap_compatible_v4_trades",
    "uniswap_compatible_general_trades",
    "balancer_compatible_v1_trades",
    "balancer_compatible_v2_trades",
    "balancer_compatible_v3_trades",
    "curve_compatible_general_trades",
    "curve_compatible_v1_trades",
    "curve_compatible_v2_trades",
    "maverick_compatible_trades",
    "maverick_compatible_v2_trades",
    "trader_joe_compatible_v2_trades",
    "trader_joe_compatible_v2_1_trades",
    "kyberswap_compatible_trades",
    "dodo_compatible_trades",
    "clipper_compatible_trades",
    "swaap_v2_compatible_trades",
    "native_compatible_trades",
    "openocean_compatible_v2_trades",
    "airswap_compatible_trades",
    "valantis_compatible_hot_trades",
    "eulerswap_compatible_trades",
    "carbon_defi_compatible_trades",
    "tessera_v_compatible_trades",
    "tapio_compatible_trades",
    "pancakeswap_compatible_infinity_cl_trades",
    "pancakeswap_compatible_infinity_lb_trades",
    "generic_spot_compatible_trades",
    "generic_spot_v2_compatible_trades",
)

# Bulk-portable families: for these, a fork is decoded by the same event
# signature as the parent. Generate template-aliased YAML.
BULK_PORT_FAMILIES: dict[str, str] = {
    "uniswap_compatible_v2_trades": "uniswap_v2",
    "uniswap_compatible_v3_trades": "uniswap_v3",
}

# Families that need pool-registry / multi-log / stateful logic — out of scope
# for bulk port; tracked as separate hand-curated tickets.
NEEDS_HAND_CURATION: frozenset[str] = frozenset(
    {
        "uniswap_compatible_v4_trades",
        "balancer_compatible_v1_trades",
        "balancer_compatible_v2_trades",
        "balancer_compatible_v3_trades",
        "curve_compatible_general_trades",
        "curve_compatible_v1_trades",
        "curve_compatible_v2_trades",
        "maverick_compatible_trades",
        "maverick_compatible_v2_trades",
        "trader_joe_compatible_v2_trades",
        "trader_joe_compatible_v2_1_trades",
        "kyberswap_compatible_trades",
        "dodo_compatible_trades",
        "clipper_compatible_trades",
        "swaap_v2_compatible_trades",
        "native_compatible_trades",
        "openocean_compatible_v2_trades",
        "airswap_compatible_trades",
        "valantis_compatible_hot_trades",
        "eulerswap_compatible_trades",
        "carbon_defi_compatible_trades",
        "tessera_v_compatible_trades",
        "tapio_compatible_trades",
        "pancakeswap_compatible_infinity_cl_trades",
        "pancakeswap_compatible_infinity_lb_trades",
        "generic_spot_compatible_trades",
        "generic_spot_v2_compatible_trades",
        "uniswap_compatible_general_trades",
    }
)


@dataclass(frozen=True)
class PlatformInvocation:
    """One macro call within a platform SQL file."""

    chain: str
    project: str
    version: str
    family: str
    source_file: Path

    @property
    def protocol_slug(self) -> str:
        """Slug used for the generated YAML filename / `protocol:` field.

        Versioned (`uniswap_v2`, `aerodrome_slipstream`) so v1/v2/v3 of the
        same project are distinct decoders.
        """
        if not self.version:
            return self.project
        return f"{self.project}_v{self.version}"


# Match a dbt macro call where the macro name is on its own line and the
# argument list spans multiple lines (the spellbook style). Captures macro
# name and the body up to the closing paren.
_MACRO_RE = re.compile(
    r"^[ \t]*(?P<name>[a-z_][a-z0-9_]*)\s*\(\s*$(?P<body>.*?)^\s*\)\s*$",
    re.MULTILINE | re.DOTALL,
)

# Within the macro body, each `key = 'value'` argument.
_KW_RE = re.compile(
    r"(?P<key>blockchain|project|version)\s*=\s*'(?P<val>[^']*)'",
)


def _parse_file(path: Path) -> list[PlatformInvocation]:
    text = path.read_text(errors="replace")
    invocations: list[PlatformInvocation] = []
    for m in _MACRO_RE.finditer(text):
        name = m.group("name")
        if name not in KNOWN_FAMILIES:
            continue
        body = m.group("body")
        kwargs: dict[str, str] = {
            kw.group("key"): kw.group("val") for kw in _KW_RE.finditer(body)
        }
        chain = kwargs.get("blockchain", "")
        project = kwargs.get("project", "")
        version = kwargs.get("version", "")
        if not chain or not project:
            continue
        invocations.append(
            PlatformInvocation(
                chain=chain,
                project=project,
                version=version,
                family=name,
                source_file=path,
            )
        )
    return invocations


def walk_platforms(
    spellbook_root: Path,
    chains: Iterable[str] = ("ethereum", "base", "arbitrum", "optimism", "polygon"),
) -> list[PlatformInvocation]:
    """Walk `dbt_subprojects/dex/models/trades/<chain>/platforms/*.sql`.

    Returns one `PlatformInvocation` per recognised macro call. A single SQL
    file can yield multiple invocations (e.g. Aerodrome on Base ships v1 and
    Slipstream in one file).
    """
    out: list[PlatformInvocation] = []
    base = spellbook_root / "dbt_subprojects" / "dex" / "models" / "trades"
    for chain in chains:
        platforms = base / chain / "platforms"
        if not platforms.is_dir():
            continue
        for sql_file in sorted(platforms.glob("*.sql")):
            out.extend(_parse_file(sql_file))
    return out


def summarise(invocations: list[PlatformInvocation]) -> dict[str, int]:
    """Count invocations grouped by family — the manifest the AC asks for."""
    counts: dict[str, int] = {}
    for inv in invocations:
        counts[inv.family] = counts.get(inv.family, 0) + 1
    return counts


def bulk_portable(invocations: list[PlatformInvocation]) -> list[PlatformInvocation]:
    """Filter to invocations whose family has a YAML template available."""
    return [inv for inv in invocations if inv.family in BULK_PORT_FAMILIES]


def deferred(invocations: list[PlatformInvocation]) -> list[PlatformInvocation]:
    """Invocations that need hand-curation (pool registry, multi-log, etc.)."""
    return [inv for inv in invocations if inv.family in NEEDS_HAND_CURATION]
