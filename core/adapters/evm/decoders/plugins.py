"""Per-protocol post-processing plugins for the generic ABI decoder.

A plugin is invoked after the generic decoder has built a `DecodedEvent`
from the YAML field bindings. It receives the partially-built event, the
decoded event arguments by name, and the raw log dict, and returns the
finalised event. Use plugins for logic that cannot be expressed
declaratively in YAML — most commonly, deciding which side of a
two-token event is the "in" leg vs the "out" leg.

When adding a new plugin, reference it from the YAML mapping with a
dotted spec like ``core.adapters.evm.decoders.plugins:my_plugin``.
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Any, Callable

from core.adapters.evm.decoders.base import DecodedEvent

# UniV4 swap decoder needs PoolId -> (currency0, currency1) lookup. The
# registry is populated by ops/backfill_uniswap_v4_pools.py from PoolManager
# Initialize events. We lazy-load it the first time a swap log is decoded —
# avoids paying the Postgres roundtrip when no UniV4 logs are present.
_UNIV4_POOL_RESOLVER: "Callable[[str, str], tuple[str, str] | None] | None" = None
_UNIV4_POOL_RESOLVER_LOADED = False


def _univ4_pool_resolver() -> "Callable[[str, str], tuple[str, str] | None] | None":
    """Lazy-load the UniV4 pool registry. Cache misses fail closed (return None)."""
    global _UNIV4_POOL_RESOLVER, _UNIV4_POOL_RESOLVER_LOADED
    if _UNIV4_POOL_RESOLVER_LOADED:
        return _UNIV4_POOL_RESOLVER
    _UNIV4_POOL_RESOLVER_LOADED = True
    try:
        from core.registry.uniswap_v4_pools import (
            UniV4PoolStore,
            make_cached_pool_resolver,
        )
        dsn = os.environ.get(
            "PROTOCOL_CONTRACTS_DSN",
            "postgresql://nexus:nexus@localhost:5434/nexus_ops",
        )
        store = UniV4PoolStore(dsn)
        if store.count() == 0:
            return None
        _UNIV4_POOL_RESOLVER = make_cached_pool_resolver(store)
    except Exception:  # noqa: BLE001 — registry is optional
        _UNIV4_POOL_RESOLVER = None
    return _UNIV4_POOL_RESOLVER


def _reset_univ4_pool_resolver() -> None:
    """Test hook — clear the cached resolver between tests."""
    global _UNIV4_POOL_RESOLVER, _UNIV4_POOL_RESOLVER_LOADED
    _UNIV4_POOL_RESOLVER = None
    _UNIV4_POOL_RESOLVER_LOADED = False


def uniswap_v2_amounts(
    event: DecodedEvent, args: dict[str, Any], log: dict[str, Any]
) -> DecodedEvent:
    """UniV2 Swap emits four uint256 amounts (amount0In/Out, amount1In/Out).

    The "in" amount is whichever of amount0In / amount1In is non-zero;
    same for "out". token0 / token1 resolution is intentionally skipped
    here — the pool registry pipeline (na-fs3p) populates token addresses
    later via ASOF join.
    """
    a0i = Decimal(args["amount0In"])
    a1i = Decimal(args["amount1In"])
    a0o = Decimal(args["amount0Out"])
    a1o = Decimal(args["amount1Out"])
    event.amount_in = a0i if a0i > 0 else a1i
    event.amount_out = a0o if a0o > 0 else a1o
    event.extra.update(
        amount0_in=str(a0i),
        amount1_in=str(a1i),
        amount0_out=str(a0o),
        amount1_out=str(a1o),
    )
    return event


def uniswap_v3_amounts(
    event: DecodedEvent, args: dict[str, Any], log: dict[str, Any]
) -> DecodedEvent:
    """UniV3 Swap emits two signed int256 deltas (amount0, amount1).

    Positive means tokens were paid into the pool (the "in" leg); negative
    means tokens were sent out. The "in" amount is the positive value;
    the "out" amount is the absolute of the negative value.
    """
    a0 = Decimal(args["amount0"])
    a1 = Decimal(args["amount1"])
    event.amount_in = a0 if a0 > 0 else a1
    event.amount_out = abs(a0) if a0 < 0 else abs(a1)
    event.extra.update(amount0=str(a0), amount1=str(a1))
    return event


def uniswap_v4_swap_amounts(
    event: DecodedEvent, args: dict[str, Any], log: dict[str, Any]
) -> DecodedEvent:
    """UniV4 Swap emits int128 deltas signed from the user's perspective.

    Negative means the user received that token; positive means the user
    paid it. Token resolution: PoolId -> (currency0, currency1) lookup
    against the `uniswap_v4_pools` registry (populated by
    ops/backfill_uniswap_v4_pools.py from PoolManager Initialize events).
    Pools not in the registry leave token_in/token_out as None and a
    later run can rerun decoding once the registry is rebuilt.
    """
    a0 = Decimal(args["amount0"])
    a1 = Decimal(args["amount1"])

    pool_id = args["id"]
    chain = log.get("chain") or "ethereum"  # decoder caller may pass through
    resolver = _univ4_pool_resolver()
    tokens = resolver(chain, pool_id) if resolver is not None else None
    currency0, currency1 = (tokens or (None, None))

    # amount0 sign tells us which currency is in vs out (user-perspective):
    #   amount0 > 0  -> user paid currency0 (currency0 is "in"), received currency1
    #   amount0 < 0  -> user received currency0 (currency0 is "out"), paid currency1
    if a0 > 0:
        event.amount_in = abs(a0)
        event.amount_out = abs(a1)
        event.token_in = currency0
        event.token_out = currency1
    else:
        event.amount_in = abs(a1)
        event.amount_out = abs(a0)
        event.token_in = currency1
        event.token_out = currency0

    event.extra.update(
        pool_id=pool_id,
        amount0=str(a0),
        amount1=str(a1),
        sqrt_price_x96=str(args.get("sqrtPriceX96", "")),
        liquidity=str(args.get("liquidity", "")),
        tick=str(args.get("tick", "")),
        fee=str(args.get("fee", "")),
    )
    return event


def uniswap_v4_initialize(
    event: DecodedEvent, args: dict[str, Any], log: dict[str, Any]
) -> DecodedEvent:
    """UniV4 Initialize event records pool creation.

    Currencies are indexed args (token0, token1). We surface them as
    token_in/token_out and stash hooks + fee + tickSpacing in extra; this
    is the seed data for a pool-registry pipeline that lets the Swap
    decoder resolve PoolId → tokens later. event_type is overridden to
    'pool_create' so it doesn't get lumped with swaps.
    """
    event.event_type = "pool_create"
    event.token_in = args.get("currency0")
    event.token_out = args.get("currency1")
    event.extra.update(
        pool_id=args["id"],
        fee=str(args.get("fee", "")),
        tick_spacing=str(args.get("tickSpacing", "")),
        hooks=args.get("hooks", ""),
    )
    return event
