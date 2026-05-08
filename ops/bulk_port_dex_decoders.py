"""Walk a spellbook clone, mine DEX platform SQLs, emit template-aliased YAMLs.

Usage:
    PYTHONPATH=. python ops/bulk_port_dex_decoders.py [--spellbook PATH] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from core.adapters.evm.spellbook.sql_miner import (
    bulk_portable,
    deferred,
    summarise,
    walk_platforms,
)
from core.adapters.evm.spellbook.yaml_emitter import (
    deferred_manifest,
    emit_template_yamls,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--spellbook",
        default=os.environ.get(
            "SPELLBOOK_PATH",
            str(Path(__file__).resolve().parents[2] / "spellbook"),
        ),
    )
    parser.add_argument(
        "--mappings-dir",
        default=str(
            Path(__file__).resolve().parents[1]
            / "core"
            / "adapters"
            / "evm"
            / "decoders"
            / "mappings"
        ),
    )
    parser.add_argument(
        "--chains",
        nargs="+",
        default=["ethereum", "base", "arbitrum", "optimism", "polygon"],
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mine + report but do not write YAML files.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing YAMLs (default: skip).",
    )
    args = parser.parse_args(argv)

    spellbook = Path(args.spellbook)
    if not (spellbook / "dbt_subprojects" / "dex" / "models" / "trades").is_dir():
        print(f"error: not a spellbook clone: {spellbook}", file=sys.stderr)
        return 2

    invs = walk_platforms(spellbook, chains=tuple(args.chains))
    print(f"mined {len(invs)} platform-macro invocations from {spellbook}")
    print("by family:")
    for fam, n in sorted(summarise(invs).items(), key=lambda kv: -kv[1]):
        print(f"  {fam:50s} {n:4d}")

    bulk = bulk_portable(invs)
    held = deferred(invs)
    print(f"\nbulk-portable: {len(bulk)}  deferred: {len(held)}")

    out_dir = Path(args.mappings_dir)
    if args.dry_run:
        print(f"\n[dry-run] would write to {out_dir}")
    else:
        written = emit_template_yamls(bulk, out_dir, overwrite=args.overwrite)
        print(f"\nwrote {len(written)} YAML files to {out_dir}")
        for p in written:
            print(f"  {p.name}")

    deferred_path = out_dir.parent / "deferred_protocols.json"
    manifest = deferred_manifest(invs)
    deferred_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(
        f"\ndeferred manifest ({len(manifest)} projects) -> {deferred_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
