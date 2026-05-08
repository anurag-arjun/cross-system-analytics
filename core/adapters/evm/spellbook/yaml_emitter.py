"""Emit `mappings/{protocol}.yaml` files from spellbook SQL miner output.

For bulk-portable families (UniV2/V3 forks), generates a 3-line template-aliased
YAML — the parent (`uniswap_v2.yaml` / `uniswap_v3.yaml`) supplies events and
plugins. For deferred families (Curve, Balancer V2, UniV4, …), emits nothing —
those need pool-registry-aware hand-curation.

Multiple chains for the same project collapse into one YAML — the file's
`chains:` field lists every chain the project ships on. This is correct
because the address-first registry lookup uses (chain, address) → protocol;
the same address is very rarely reused across chains, so chain-level
discrimination is implicit in the address universe.
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from core.adapters.evm.spellbook.sql_miner import (
    BULK_PORT_FAMILIES,
    PlatformInvocation,
)


def emit_template_yamls(
    invocations: list[PlatformInvocation],
    out_dir: Path,
    overwrite: bool = False,
) -> list[Path]:
    """Write one YAML per (project, version) for bulk-portable families.

    Returns the list of paths written. Skips files that already exist unless
    `overwrite=True`. Hand-written mappings (e.g. uniswap_v2.yaml itself) are
    never overwritten — by convention they have no `template:` field.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    grouped: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for inv in invocations:
        if inv.family not in BULK_PORT_FAMILIES:
            continue
        template = BULK_PORT_FAMILIES[inv.family]
        key = (inv.project, inv.version, template)
        if inv.chain not in grouped[key]:
            grouped[key].append(inv.chain)

    written: list[Path] = []
    for (project, version, template), chains in sorted(grouped.items()):
        # Don't overwrite hand-curated parents (e.g. uniswap_v2.yaml).
        slug = f"{project}_v{version}" if version else project
        if slug == template:
            continue

        path = out_dir / f"{slug}.yaml"
        if path.exists() and not overwrite:
            continue

        chains_yaml = "[" + ", ".join(sorted(chains)) + "]"
        body = (
            f"protocol: {slug}\n"
            f"chains: {chains_yaml}\n"
            f"template: {template}\n"
        )
        path.write_text(body)
        written.append(path)
    return written


def deferred_manifest(invocations: list[PlatformInvocation]) -> list[dict]:
    """Return a list of {project, version, family, chains} dicts for deferred work.

    Useful for filing follow-up tickets: each entry corresponds to one
    project that needs hand-curation (pool-registry pipeline + bespoke YAML).
    """
    grouped: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    for inv in invocations:
        if inv.family in BULK_PORT_FAMILIES:
            continue
        key = (inv.project, inv.version, inv.family)
        if inv.chain not in grouped[key]:
            grouped[key].append(inv.chain)
    return [
        {
            "project": project,
            "version": version,
            "family": family,
            "chains": sorted(chains),
        }
        for (project, version, family), chains in sorted(grouped.items())
    ]
