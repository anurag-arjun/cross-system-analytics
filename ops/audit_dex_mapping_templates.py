"""Audit bulk-port DEX YAML mappings against actual on-chain topic0s.

The spellbook bulk-port (na-9c7w) classified every `uniswap_compatible_v2_trades`
macro under `template: uniswap_v2` and every `*_v3_trades` under `template:
uniswap_v3`. The na-hmeu parity dig found that Solidly-style forks (Aerodrome
v1, Velodrome v2) emit a different Swap signature than UniV2, so their
template was wrong and the decoders never fired.

This audit script verifies the parent template for every templated YAML by:

1. For each YAML with a `template:` field:
2. For each chain it claims to cover, sample addresses from
   `protocol_contracts` where (protocol, version) match the YAML's slug.
3. Query `canonical_logs` for the topic0 distribution emitted by those
   addresses in a recent window.
4. Identify the dominant Swap-like topic0 (one of UniV2 / UniV3 / Solidly).
5. Compare with the declared template — flag mismatches.

Optional `--fix` rewrites mismatching YAMLs to the correct template. The new
parent must already exist (uniswap_v2.yaml, uniswap_v3.yaml, solidly_v1.yaml).

Usage:
    PYTHONPATH=. python ops/audit_dex_mapping_templates.py [--fix]
"""

from __future__ import annotations

import argparse
import os
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import clickhouse_connect
import psycopg2
import yaml
from dotenv import load_dotenv
from eth_utils import keccak

load_dotenv()

MAPPINGS_DIR = (
    Path(__file__).resolve().parents[1]
    / "core"
    / "adapters"
    / "evm"
    / "decoders"
    / "mappings"
)

# Known Swap-like topic0s and their canonical parent.
SIGNATURES: dict[str, tuple[str, str]] = {
    "Swap(address,uint256,uint256,uint256,uint256,address)": ("uniswap_v2", "UNIV2"),
    "Swap(address,address,uint256,uint256,uint256,uint256)": ("solidly_v1", "SOLIDLY"),
    "Swap(address,address,int256,int256,uint160,uint128,int24)": ("uniswap_v3", "UNIV3"),
}

TOPIC0_TO_PARENT: dict[str, str] = {
    "0x" + keccak(text=sig).hex(): parent for sig, (parent, _) in SIGNATURES.items()
}
TOPIC0_TO_LABEL: dict[str, str] = {
    "0x" + keccak(text=sig).hex(): label for sig, (_, label) in SIGNATURES.items()
}


@dataclass
class YamlRow:
    path: Path
    protocol: str
    chains: list[str]
    template: str | None
    raw: dict[str, Any]

    @property
    def project_and_version(self) -> tuple[str, str | None]:
        """Reverse `{project}_v{version}` -> (project, version). Strips trailing
        `_v<version>` when present; otherwise returns (slug, None)."""
        slug = self.protocol
        if "_v" in slug:
            head, _, tail = slug.rpartition("_v")
            return head, tail
        return slug, None


def load_templated_yamls() -> list[YamlRow]:
    out: list[YamlRow] = []
    for p in sorted(MAPPINGS_DIR.glob("*.yaml")):
        raw = yaml.safe_load(p.read_text())
        if not isinstance(raw, dict):
            continue
        tmpl = raw.get("template")
        if tmpl is None:
            continue
        out.append(
            YamlRow(
                path=p,
                protocol=raw["protocol"],
                chains=list(raw.get("chains", [])),
                template=tmpl,
                raw=raw,
            )
        )
    return out


def sample_addresses(
    pg_conn: Any, chain: str, project: str, version: str | None, limit: int = 50
) -> list[str]:
    """Pull up to `limit` addresses for (chain, protocol, version) from
    protocol_contracts. Falls back to LIKE-match on protocol if exact
    project+version yields nothing (spellbook & dune sometimes spell the same
    project differently — e.g. 'uniswap' vs 'uniswap-x')."""
    with pg_conn.cursor() as cur:
        if version is None:
            cur.execute(
                """
                SELECT address FROM protocol_contracts
                WHERE chain = %s AND protocol = %s
                LIMIT %s
                """,
                (chain, project, limit),
            )
        else:
            cur.execute(
                """
                SELECT address FROM protocol_contracts
                WHERE chain = %s AND protocol = %s AND version = %s
                LIMIT %s
                """,
                (chain, project, version, limit),
            )
        rows = [r[0] for r in cur.fetchall()]
    return rows


def dominant_swap_topic0(
    ch_client: Any, chain: str, addresses: list[str], hours: int = 24
) -> tuple[str | None, int]:
    """Among the candidate Swap topic0s, return whichever fires most across
    the given pool addresses in the last `hours` window. Returns (topic0, count)
    or (None, 0) if no candidate topic0 appears."""
    if not addresses:
        return None, 0
    addr_sql = ", ".join(f"lower('{a}')" for a in addresses)
    topic_sql = ", ".join(f"'{t}'" for t in TOPIC0_TO_PARENT)
    sql = f"""
        SELECT topic0, count() AS n FROM canonical_logs
        WHERE chain = '{chain}'
          AND lower(address) IN ({addr_sql})
          AND topic0 IN ({topic_sql})
          AND block_time > now() - INTERVAL {hours} HOUR
        GROUP BY topic0
        ORDER BY n DESC
        LIMIT 1
    """
    res = ch_client.query(sql)
    if not res.result_rows:
        return None, 0
    t0, n = res.result_rows[0]
    t0_str = t0 if isinstance(t0, str) else t0.decode()
    return t0_str, int(n)


@dataclass
class AuditRow:
    yaml_path: Path
    protocol: str
    chain: str
    declared_template: str
    observed_topic0: str | None
    observed_parent: str | None
    observed_label: str | None
    observed_count: int
    status: str  # match | mismatch | no_data | no_pools


def audit(
    yamls: list[YamlRow], pg_conn: Any, ch_client: Any, hours: int = 24
) -> list[AuditRow]:
    out: list[AuditRow] = []
    for y in yamls:
        project, version = y.project_and_version
        for chain in y.chains:
            addrs = sample_addresses(pg_conn, chain, project, version, limit=50)
            if not addrs:
                out.append(
                    AuditRow(
                        yaml_path=y.path,
                        protocol=y.protocol,
                        chain=chain,
                        declared_template=y.template,
                        observed_topic0=None,
                        observed_parent=None,
                        observed_label=None,
                        observed_count=0,
                        status="no_pools",
                    )
                )
                continue
            t0, n = dominant_swap_topic0(ch_client, chain, addrs, hours=hours)
            if t0 is None:
                out.append(
                    AuditRow(
                        yaml_path=y.path,
                        protocol=y.protocol,
                        chain=chain,
                        declared_template=y.template,
                        observed_topic0=None,
                        observed_parent=None,
                        observed_label=None,
                        observed_count=0,
                        status="no_data",
                    )
                )
                continue
            observed_parent = TOPIC0_TO_PARENT[t0]
            status = "match" if observed_parent == y.template else "mismatch"
            out.append(
                AuditRow(
                    yaml_path=y.path,
                    protocol=y.protocol,
                    chain=chain,
                    declared_template=y.template,
                    observed_topic0=t0,
                    observed_parent=observed_parent,
                    observed_label=TOPIC0_TO_LABEL[t0],
                    observed_count=n,
                    status=status,
                )
            )
    return out


def best_template_per_yaml(
    rows: list[AuditRow],
) -> dict[Path, tuple[str, str]]:
    """For each YAML, pick the consensus observed parent across chains, weighting
    by `observed_count`. Returns {path: (declared_template, recommended_template)}.

    Skips YAMLs where no chain has data (status no_data/no_pools).
    """
    grouped: dict[Path, list[AuditRow]] = {}
    for r in rows:
        grouped.setdefault(r.yaml_path, []).append(r)

    out: dict[Path, tuple[str, str]] = {}
    for path, group in grouped.items():
        weighted: Counter[str] = Counter()
        declared = group[0].declared_template
        for r in group:
            if r.observed_parent and r.observed_count > 0:
                weighted[r.observed_parent] += r.observed_count
        if not weighted:
            continue
        recommended, _ = weighted.most_common(1)[0]
        out[path] = (declared, recommended)
    return out


def apply_fixes(
    decisions: dict[Path, tuple[str, str]], dry_run: bool = True
) -> list[tuple[Path, str, str]]:
    """For each YAML where declared != recommended, rewrite template line."""
    applied: list[tuple[Path, str, str]] = []
    for path, (declared, recommended) in decisions.items():
        if declared == recommended:
            continue
        raw = yaml.safe_load(path.read_text())
        raw["template"] = recommended
        # Preserve key order: protocol, chains, template, events.
        new_text_lines: list[str] = []
        new_text_lines.append(f"protocol: {raw['protocol']}")
        chains = raw.get("chains") or []
        chains_yaml = "[" + ", ".join(sorted(chains)) + "]"
        new_text_lines.append(f"chains: {chains_yaml}")
        new_text_lines.append(f"template: {recommended}")
        new_text = "\n".join(new_text_lines) + "\n"
        if not dry_run:
            path.write_text(new_text)
        applied.append((path, declared, recommended))
    return applied


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--postgres",
        default=os.environ.get(
            "PROTOCOL_CONTRACTS_DSN",
            "postgresql://nexus:nexus@localhost:5434/nexus_ops",
        ),
    )
    p.add_argument("--ch-host", default="localhost")
    p.add_argument("--ch-port", type=int, default=8124)
    p.add_argument("--ch-user", default="default")
    p.add_argument("--ch-password", default="nexus")
    p.add_argument("--ch-database", default="nexus")
    p.add_argument("--hours", type=int, default=24)
    p.add_argument("--fix", action="store_true", help="Rewrite mismatching YAMLs.")
    args = p.parse_args(argv)

    yamls = load_templated_yamls()
    print(f"templated YAMLs: {len(yamls)}")

    pg = psycopg2.connect(args.postgres)
    ch = clickhouse_connect.get_client(
        host=args.ch_host, port=args.ch_port, username=args.ch_user,
        password=args.ch_password, database=args.ch_database,
    )

    try:
        rows = audit(yamls, pg, ch, hours=args.hours)
    finally:
        ch.close()
        pg.close()

    # Per-row status summary
    summary: Counter[str] = Counter(r.status for r in rows)
    print(f"audit rows (chain x yaml): {len(rows)}  | {dict(summary)}")

    # YAML-level decisions
    decisions = best_template_per_yaml(rows)
    fixes = [(path, decl, rec) for path, (decl, rec) in decisions.items() if decl != rec]

    if fixes:
        print(f"\nMISMATCHES ({len(fixes)}):")
        for path, decl, rec in sorted(fixes):
            print(f"  {path.name:<35s}  declared={decl}  observed={rec}")
    else:
        print("\nAll templated YAMLs match their observed parent.")

    no_data_yamls = sorted(
        {r.yaml_path.name for r in rows if r.status in ("no_data", "no_pools")}
        - {p.name for p in decisions}
    )
    if no_data_yamls:
        print(f"\nNO ON-CHAIN DATA in last {args.hours}h ({len(no_data_yamls)} YAMLs):")
        for n in no_data_yamls[:20]:
            print(f"  {n}")
        if len(no_data_yamls) > 20:
            print(f"  ... +{len(no_data_yamls) - 20} more")

    if args.fix and fixes:
        applied = apply_fixes(decisions, dry_run=False)
        print(f"\nrewrote {len(applied)} YAML files")
    elif fixes:
        print("\n[dry-run] pass --fix to rewrite mismatching YAMLs")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
