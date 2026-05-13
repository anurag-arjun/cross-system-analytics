"""Bridge transaction status classifier.

The bridge explorer surfaces three row types:
  - `pair`       : a matched bridge_out/bridge_in (from bridge_links)
  - `orphan_out` : a bridge_out with no matching bridge_in
  - `orphan_in`  : a bridge_in with no matching bridge_out

`classify()` assigns each row a primary `status` (one of the Status enum)
plus zero or more data-quality `tags` (Tag enum) and a human-readable
`reason`. The classifier is pure: given a row dict + the current time +
a config dict, the output is deterministic.

Primary statuses are mutually exclusive; tags are independent flags on
top of MATCHED that flag "this row needs a closer look".
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from core.identity.chain_mapping import CHAIN_ID_TO_CHAIN, EID_TO_CHAIN


def _to_utc(dt: datetime) -> datetime:
    """Treat naive datetimes from ClickHouse as UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

MONITORED_CHAINS = {"ethereum", "base", "arbitrum", "optimism", "polygon"}


class Status(str, Enum):
    MATCHED = "MATCHED"
    PENDING_FINALITY = "PENDING_FINALITY"
    IN_FLIGHT = "IN_FLIGHT"
    UNMATCHED_DST_OUT_OF_SCOPE = "UNMATCHED_DST_OUT_OF_SCOPE"
    UNMATCHED_SRC_OUT_OF_SCOPE = "UNMATCHED_SRC_OUT_OF_SCOPE"
    UNMATCHED_DECODER_GAP = "UNMATCHED_DECODER_GAP"
    UNMATCHED_BROKEN_MATCHER = "UNMATCHED_BROKEN_MATCHER"
    UNMATCHED_UNKNOWN = "UNMATCHED_UNKNOWN"


class Tag(str, Enum):
    AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
    LATENCY_OUTLIER = "LATENCY_OUTLIER"
    NEGATIVE_LATENCY = "NEGATIVE_LATENCY"
    SAME_CHAIN = "SAME_CHAIN"
    MULTI_MATCH = "MULTI_MATCH"
    RECIPIENT_DIFFERS = "RECIPIENT_DIFFERS"
    NO_USD_VALUE = "NO_USD_VALUE"
    TOKEN_CHANGED = "TOKEN_CHANGED"


@dataclass(frozen=True)
class BridgeConfig:
    typical_latency_seconds: int           # happy-path bridge_in arrival
    finality_seconds: int                  # native bridges have a known waiting period
    bridge_out_decoded: bool                # do we decode the source side?
    bridge_in_decoded: bool                 # do we decode the destination side?
    matcher_broken: bool = False           # link_key scheme can't link cross-chain
    amount_fee_tolerance_pct: float = 5.0  # gap > this → AMOUNT_MISMATCH
    expects_token_change: bool = False     # OFTs/swap-bridges legitimately change tokens


# Per-bridge profile. Finality numbers are documented protocol-side latencies
# (op_stack / arbitrum_bridge 7-day withdrawal challenge). Typical-latency
# numbers are observed p50 + headroom for the happy path.
_BRIDGES: dict[str, BridgeConfig] = {
    "across":          BridgeConfig(typical_latency_seconds=180,  finality_seconds=600,     bridge_out_decoded=True,  bridge_in_decoded=True,                                    amount_fee_tolerance_pct=2.0),
    "layerzero":       BridgeConfig(typical_latency_seconds=600,  finality_seconds=1800,    bridge_out_decoded=True,  bridge_in_decoded=True,                                    amount_fee_tolerance_pct=99.0, expects_token_change=True),
    "stargate":        BridgeConfig(typical_latency_seconds=600,  finality_seconds=1800,    bridge_out_decoded=True,  bridge_in_decoded=False),
    "cctp":            BridgeConfig(typical_latency_seconds=1200, finality_seconds=1800,    bridge_out_decoded=True,  bridge_in_decoded=False, amount_fee_tolerance_pct=1.0),
    "op_stack":        BridgeConfig(typical_latency_seconds=600,  finality_seconds=7*86400, bridge_out_decoded=True,  bridge_in_decoded=True,  matcher_broken=True),
    "arbitrum_bridge": BridgeConfig(typical_latency_seconds=600,  finality_seconds=7*86400, bridge_out_decoded=False, bridge_in_decoded=True),
    "base_native":     BridgeConfig(typical_latency_seconds=600,  finality_seconds=7*86400, bridge_out_decoded=True,  bridge_in_decoded=False),
}

_DEFAULT_CFG = BridgeConfig(
    typical_latency_seconds=600, finality_seconds=3600,
    bridge_out_decoded=True, bridge_in_decoded=True,
)


def classify(
    row: dict[str, Any],
    *,
    now: datetime,
    bridges: dict[str, BridgeConfig] | None = None,
) -> dict[str, Any]:
    """Return {'status', 'tags', 'reason'} for an explorer row."""
    cfg_map = bridges if bridges is not None else _BRIDGES
    bridge_name = (row.get("bridge") or "").lower()
    cfg = cfg_map.get(bridge_name, _DEFAULT_CFG)
    row_type = row.get("row_type")

    if row_type == "pair":
        return _classify_pair(row, cfg)
    if row_type == "orphan_out":
        return _classify_orphan_out(row, cfg, bridge_name, now)
    if row_type == "orphan_in":
        return _classify_orphan_in(row, cfg, bridge_name, now)
    return {
        "status": Status.UNMATCHED_UNKNOWN.value,
        "tags": [],
        "reason": f"unknown row_type: {row_type!r}",
    }


def _classify_pair(row: dict, cfg: BridgeConfig) -> dict[str, Any]:
    tags: list[str] = []

    latency = row.get("latency_seconds")
    if latency is not None:
        if latency < 0:
            tags.append(Tag.NEGATIVE_LATENCY.value)
        elif latency > max(cfg.typical_latency_seconds * 10, cfg.finality_seconds):
            tags.append(Tag.LATENCY_OUTLIER.value)

    if row.get("src_chain") and row.get("src_chain") == row.get("dst_chain"):
        tags.append(Tag.SAME_CHAIN.value)

    src_e = (row.get("src_entity_id") or "").lower()
    dst_e = (row.get("dst_entity_id") or "").lower()
    if src_e and dst_e and src_e != dst_e:
        tags.append(Tag.RECIPIENT_DIFFERS.value)

    src_amt = _f(row.get("src_amount"))
    dst_amt = _f(row.get("dst_amount"))
    if src_amt > 0 and dst_amt > 0:
        diff_pct = abs(src_amt - dst_amt) / src_amt * 100
        if diff_pct > cfg.amount_fee_tolerance_pct:
            tags.append(Tag.AMOUNT_MISMATCH.value)

    if row.get("src_amount_usd") is None and row.get("dst_amount_usd") is None:
        tags.append(Tag.NO_USD_VALUE.value)

    src_tok = (row.get("src_token") or "").lower()
    dst_tok = (row.get("dst_token") or "").lower()
    # Token addresses always differ across chains (USDC on eth ≠ USDC on base),
    # so TOKEN_CHANGED only means something for same-chain bridges. Cross-chain
    # token sameness requires symbol-level resolution we don't have here.
    same_chain = row.get("src_chain") and row.get("src_chain") == row.get("dst_chain")
    if (
        same_chain
        and src_tok
        and dst_tok
        and src_tok != dst_tok
        and not cfg.expects_token_change
    ):
        tags.append(Tag.TOKEN_CHANGED.value)

    if row.get("multi_match_src") or row.get("multi_match_dst"):
        tags.append(Tag.MULTI_MATCH.value)

    return {"status": Status.MATCHED.value, "tags": tags, "reason": ""}


def _classify_orphan_out(
    row: dict, cfg: BridgeConfig, bridge_name: str, now: datetime,
) -> dict[str, Any]:
    dst_hint = row.get("dst_chain_id_hint")
    dst_chain_name = _resolve_chain(dst_hint, row.get("link_key_type"))
    if dst_hint and dst_chain_name not in MONITORED_CHAINS:
        label = dst_chain_name or f"id={dst_hint}"
        return {
            "status": Status.UNMATCHED_DST_OUT_OF_SCOPE.value,
            "tags": [],
            "reason": f"dst={label} not ingested",
        }

    if not cfg.bridge_in_decoded:
        return {
            "status": Status.UNMATCHED_DECODER_GAP.value,
            "tags": [],
            "reason": f"{bridge_name} bridge_in decoder not implemented",
        }

    if cfg.matcher_broken:
        return {
            "status": Status.UNMATCHED_BROKEN_MATCHER.value,
            "tags": [],
            "reason": f"{bridge_name} link_key uses local tx_hash on both sides — cannot match cross-chain",
        }

    src_ts = row.get("src_block_time")
    if isinstance(src_ts, datetime):
        age = (now - _to_utc(src_ts)).total_seconds()
        if age < cfg.finality_seconds and cfg.finality_seconds > cfg.typical_latency_seconds * 10:
            return {
                "status": Status.PENDING_FINALITY.value,
                "tags": [],
                "reason": f"within {cfg.finality_seconds // 60}-min finality window",
            }
        if age < cfg.typical_latency_seconds * 3:
            return {
                "status": Status.IN_FLIGHT.value,
                "tags": [],
                "reason": f"within {cfg.typical_latency_seconds}s typical-latency window",
            }

    return {
        "status": Status.UNMATCHED_UNKNOWN.value,
        "tags": [],
        "reason": "no matching bridge_in; investigate decoder or data gap",
    }


def _classify_orphan_in(
    row: dict, cfg: BridgeConfig, bridge_name: str, now: datetime,
) -> dict[str, Any]:
    src_hint = row.get("src_chain_id_hint")
    src_chain_name = _resolve_chain(src_hint, row.get("link_key_type"))
    if src_hint and src_chain_name not in MONITORED_CHAINS:
        label = src_chain_name or f"id={src_hint}"
        return {
            "status": Status.UNMATCHED_SRC_OUT_OF_SCOPE.value,
            "tags": [],
            "reason": f"src={label} not ingested",
        }

    if not cfg.bridge_out_decoded:
        return {
            "status": Status.UNMATCHED_DECODER_GAP.value,
            "tags": [],
            "reason": f"{bridge_name} bridge_out decoder not implemented",
        }

    if cfg.matcher_broken:
        return {
            "status": Status.UNMATCHED_BROKEN_MATCHER.value,
            "tags": [],
            "reason": f"{bridge_name} link_key uses local tx_hash on both sides — cannot match cross-chain",
        }

    return {
        "status": Status.UNMATCHED_UNKNOWN.value,
        "tags": [],
        "reason": "no matching bridge_out; investigate decoder or data gap",
    }


def _resolve_chain(hint: Any, link_key_type: Any) -> str | None:
    if hint is None or hint == "":
        return None
    try:
        n = int(hint)
    except (ValueError, TypeError):
        return None
    if link_key_type == "layerzero_src_eid":
        return EID_TO_CHAIN.get(n)
    return CHAIN_ID_TO_CHAIN.get(n)


def _f(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0
