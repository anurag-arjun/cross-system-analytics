"""Unit tests for the generic ABI-driven decoder.

Asserts that YAML-driven decoders for UniV2 and UniV3 produce the same
DecodedEvent structure as the hand-written classes they replaced.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from eth_abi import encode

from core.adapters.evm.decoders.generic import (
    EventInput,
    EventMapping,
    GenericABIDecoder,
    ProtocolMapping,
    load_mapping_dir,
)
from core.adapters.evm.registry import build_default_registry

MAPPINGS_DIR = Path(__file__).parents[2] / "adapters" / "evm" / "decoders" / "mappings"

UNIV2_TOPIC0 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
UNIV3_TOPIC0 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

SENDER = "0x" + "aa" * 20
RECIPIENT = "0x" + "bb" * 20
POOL = "0x" + "cc" * 20
TX = "0x" + "de" * 32
TS = datetime(2026, 5, 8, 12, 0, 0, tzinfo=timezone.utc)


def _addr_topic(addr: str) -> str:
    return "0x" + addr[2:].rjust(64, "0")


def _load_decoder(protocol: str, topic0: str) -> GenericABIDecoder:
    for mapping in load_mapping_dir(MAPPINGS_DIR):
        if mapping.protocol != protocol:
            continue
        for event in mapping.events:
            decoder = GenericABIDecoder(mapping, event)
            if decoder.topic0 == topic0:
                return decoder
    raise AssertionError(f"No decoder for {protocol} topic0={topic0}")


def test_topic0_matches_keccak():
    """Topic0 derivation matches the published UniV2/V3 hashes."""
    assert _load_decoder("uniswap_v2", UNIV2_TOPIC0).topic0 == UNIV2_TOPIC0
    assert _load_decoder("uniswap_v3", UNIV3_TOPIC0).topic0 == UNIV3_TOPIC0


def test_uniswap_v2_decode():
    decoder = _load_decoder("uniswap_v2", UNIV2_TOPIC0)
    data = encode(["uint256", "uint256", "uint256", "uint256"], [1000, 0, 0, 950]).hex()
    log = {
        "address": POOL,
        "topics": [UNIV2_TOPIC0, _addr_topic(SENDER), _addr_topic(RECIPIENT)],
        "data": "0x" + data,
        "blockNumber": "0x1234",
        "transactionHash": TX,
        "logIndex": "0x1",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.event_type == "swap"
    assert ev.protocol == "uniswap_v2"
    assert ev.entity_id == SENDER
    assert ev.venue == POOL
    assert ev.amount_in == Decimal(1000)
    assert ev.amount_out == Decimal(950)
    assert ev.extra["amount0_in"] == "1000"
    assert ev.extra["amount1_out"] == "950"
    assert ev.extra["to"] == RECIPIENT


def test_uniswap_v2_amount_picks_nonzero_side():
    """When amount1In is nonzero (token1 is the input), pick amount1In."""
    decoder = _load_decoder("uniswap_v2", UNIV2_TOPIC0)
    data = encode(
        ["uint256", "uint256", "uint256", "uint256"], [0, 5_000_000, 9_900_000, 0]
    ).hex()
    log = {
        "address": POOL,
        "topics": [UNIV2_TOPIC0, _addr_topic(SENDER), _addr_topic(RECIPIENT)],
        "data": "0x" + data,
        "blockNumber": "0x1234",
        "transactionHash": TX,
        "logIndex": "0x1",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.amount_in == Decimal(5_000_000)
    assert ev.amount_out == Decimal(9_900_000)


def test_uniswap_v3_decode():
    decoder = _load_decoder("uniswap_v3", UNIV3_TOPIC0)
    # Positive amount0 = token0 paid in; negative amount1 = token1 sent out.
    data = encode(
        ["int256", "int256", "uint160", "uint128", "int24"],
        [1000, -950, 79228162514264337593543950336, 0, 1],
    ).hex()
    log = {
        "address": POOL,
        "topics": [UNIV3_TOPIC0, _addr_topic(SENDER), _addr_topic(RECIPIENT)],
        "data": "0x" + data,
        "blockNumber": "0x1234",
        "transactionHash": TX,
        "logIndex": "0x2",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.event_type == "swap"
    assert ev.protocol == "uniswap_v3"
    assert ev.entity_id == SENDER
    assert ev.venue == POOL
    assert ev.amount_in == Decimal(1000)
    assert ev.amount_out == Decimal(950)
    assert ev.extra["amount0"] == "1000"
    assert ev.extra["amount1"] == "-950"
    assert ev.extra["recipient"] == RECIPIENT


def test_uniswap_v3_decode_reverse_direction():
    """When amount1 is positive and amount0 is negative, sides flip."""
    decoder = _load_decoder("uniswap_v3", UNIV3_TOPIC0)
    data = encode(
        ["int256", "int256", "uint160", "uint128", "int24"],
        [-2000, 2200, 79228162514264337593543950336, 0, 1],
    ).hex()
    log = {
        "address": POOL,
        "topics": [UNIV3_TOPIC0, _addr_topic(SENDER), _addr_topic(RECIPIENT)],
        "data": "0x" + data,
        "blockNumber": "0x1234",
        "transactionHash": TX,
        "logIndex": "0x2",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.amount_in == Decimal(2200)
    assert ev.amount_out == Decimal(2000)


def test_decode_returns_none_when_topics_missing():
    decoder = _load_decoder("uniswap_v3", UNIV3_TOPIC0)
    log = {
        "address": POOL,
        "topics": [UNIV3_TOPIC0],  # missing indexed args
        "data": "0x",
        "blockNumber": "0x1234",
        "transactionHash": TX,
        "logIndex": "0x2",
    }
    assert decoder.decode(log, TS) is None


def test_default_registry_includes_yaml_protocols():
    registry = build_default_registry()
    decoder_v2 = registry.lookup(UNIV2_TOPIC0, address=POOL)
    decoder_v3 = registry.lookup(UNIV3_TOPIC0, address=POOL)
    assert decoder_v2 is not None and decoder_v2.protocol == "uniswap_v2"
    assert decoder_v3 is not None and decoder_v3.protocol == "uniswap_v3"


def test_address_first_resolution_picks_correct_protocol():
    """When two protocols share the same topic0, the protocol resolver
    decides which decoder applies based on the contract address."""
    pancake_pool = "0x" + "11" * 20
    sushi_pool = "0x" + "22" * 20

    inputs = (
        EventInput(name="sender", type="address", indexed=True),
        EventInput(name="amount0In", type="uint256"),
        EventInput(name="amount1In", type="uint256"),
        EventInput(name="amount0Out", type="uint256"),
        EventInput(name="amount1Out", type="uint256"),
        EventInput(name="to", type="address", indexed=True),
    )
    canonical = {"event_type": "swap", "entity_id": "{sender}", "venue": "{log.address}"}
    plugin = "core.adapters.evm.decoders.plugins:uniswap_v2_amounts"

    pancake = ProtocolMapping(
        protocol="pancake_v2",
        chains=("base",),
        events=(
            EventMapping(name="Swap", inputs=inputs, canonical=canonical, plugin=plugin),
        ),
    )
    sushi = ProtocolMapping(
        protocol="sushiswap_v2",
        chains=("base",),
        events=(
            EventMapping(name="Swap", inputs=inputs, canonical=canonical, plugin=plugin),
        ),
    )

    pancake_decoder = GenericABIDecoder(pancake, pancake.events[0])
    sushi_decoder = GenericABIDecoder(sushi, sushi.events[0])

    from core.adapters.evm.registry import DecoderRegistry, ProtocolEntry

    address_to_protocol = {
        ("base", pancake_pool.lower()): "pancake_v2",
        ("base", sushi_pool.lower()): "sushiswap_v2",
    }

    def resolver(chain: str, address: str) -> str | None:
        return address_to_protocol.get((chain, address))

    reg = DecoderRegistry(protocol_resolver=resolver)
    reg.register(
        ProtocolEntry(
            protocol="pancake_v2",
            version="1",
            chain="*",
            topic0=pancake_decoder.topic0,
            address_pattern="*",
            decoder=pancake_decoder,
        )
    )
    reg.register(
        ProtocolEntry(
            protocol="sushiswap_v2",
            version="1",
            chain="*",
            topic0=sushi_decoder.topic0,
            address_pattern="*",
            decoder=sushi_decoder,
        )
    )

    pancake_match = reg.lookup(pancake_decoder.topic0, address=pancake_pool, chain="base")
    sushi_match = reg.lookup(sushi_decoder.topic0, address=sushi_pool, chain="base")
    unknown_match = reg.lookup(pancake_decoder.topic0, address="0x" + "99" * 20, chain="base")

    assert pancake_match is pancake_decoder
    assert sushi_match is sushi_decoder
    # Unknown address falls back to topic0 lookup; first registered wins.
    assert unknown_match is pancake_decoder


def test_template_inherits_events(tmp_path):
    """A YAML with `template: <protocol>` reuses the parent's events."""
    from core.adapters.evm.decoders.generic import load_mapping_dir

    (tmp_path / "uniswap_v2.yaml").write_text(
        "protocol: uniswap_v2\n"
        "chains: [ethereum]\n"
        "events:\n"
        "  - name: Swap\n"
        "    inputs:\n"
        "      - {name: sender, type: address, indexed: true}\n"
        "      - {name: amount0In, type: uint256}\n"
        "      - {name: amount1In, type: uint256}\n"
        "      - {name: amount0Out, type: uint256}\n"
        "      - {name: amount1Out, type: uint256}\n"
        "      - {name: to, type: address, indexed: true}\n"
        "    canonical:\n"
        "      event_type: swap\n"
        "      entity_id: \"{sender}\"\n"
        "      venue: \"{log.address}\"\n"
    )
    (tmp_path / "aerodrome.yaml").write_text(
        "protocol: aerodrome\n"
        "chains: [base]\n"
        "template: uniswap_v2\n"
    )

    mappings = {m.protocol: m for m in load_mapping_dir(tmp_path)}
    assert "aerodrome" in mappings
    assert "uniswap_v2" in mappings

    parent = mappings["uniswap_v2"]
    child = mappings["aerodrome"]
    assert child.events == parent.events
    assert child.protocol == "aerodrome"
    assert child.chains == ("base",)


def test_template_unknown_parent_raises(tmp_path):
    from core.adapters.evm.decoders.generic import load_mapping_dir

    (tmp_path / "child.yaml").write_text(
        "protocol: child\n"
        "chains: [ethereum]\n"
        "template: nonexistent\n"
    )
    try:
        load_mapping_dir(tmp_path)
    except ValueError as e:
        assert "nonexistent" in str(e)
    else:
        raise AssertionError("expected ValueError for unknown template")


def test_template_cycle_raises(tmp_path):
    from core.adapters.evm.decoders.generic import load_mapping_dir

    (tmp_path / "a.yaml").write_text("protocol: a\nchains: [ethereum]\ntemplate: b\n")
    (tmp_path / "b.yaml").write_text("protocol: b\nchains: [ethereum]\ntemplate: a\n")
    try:
        load_mapping_dir(tmp_path)
    except ValueError as e:
        assert "Cyclic" in str(e) or "cycle" in str(e).lower()
    else:
        raise AssertionError("expected ValueError for template cycle")


def test_uniswap_v4_swap_decoder():
    """UniV4 Swap: int128 amounts (signed from user perspective), bytes32 PoolId."""
    from eth_abi import encode
    from core.adapters.evm.decoders.generic import load_mapping_dir

    mappings = {m.protocol: m for m in load_mapping_dir(MAPPINGS_DIR)}
    assert "uniswap_v4" in mappings
    swap_event = next(e for e in mappings["uniswap_v4"].events if e.name == "Swap")
    decoder = GenericABIDecoder(mappings["uniswap_v4"], swap_event)
    expected_topic0 = (
        "0x"
        + __import__("eth_utils").keccak(text="Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)").hex()
    )
    assert decoder.topic0 == expected_topic0

    pool_id = "0x" + "ab" * 32
    sender = "0x" + "cc" * 20

    # int128 amounts: amount0 negative (user received token0), amount1 positive (user paid token1).
    a0 = -(123 * 10**18)
    a1 = 100 * 10**6
    sqrt_price = 79228162514264337593543950336
    liquidity = 1_000_000
    tick = -200
    fee = 3000

    data = "0x" + encode(
        ["int128", "int128", "uint160", "uint128", "int24", "uint24"],
        [a0, a1, sqrt_price, liquidity, tick, fee],
    ).hex()
    log = {
        "address": "0x000000000004444c5dc75cb358380d2e3de08a90",
        "topics": [decoder.topic0, pool_id, _addr_topic(sender)],
        "data": data,
        "blockNumber": "0x1",
        "transactionHash": TX,
        "logIndex": "0x0",
    }
    decoded = decoder.decode(log, TS)
    assert decoded is not None
    assert decoded.protocol == "uniswap_v4"
    assert decoded.event_type == "swap"
    assert decoded.entity_id == sender
    assert decoded.venue == "0x000000000004444c5dc75cb358380d2e3de08a90"
    # User paid token1 (amount1 positive) -> amount_in = abs(a1).
    # User received token0 (amount0 negative) -> amount_out = abs(a0).
    assert decoded.amount_in == abs(a1)
    assert decoded.amount_out == abs(a0)
    assert decoded.extra["pool_id"] == pool_id
    assert decoded.extra["amount0"] == str(a0)
    assert decoded.extra["fee"] == str(fee)


def test_uniswap_v4_initialize_decoder():
    """UniV4 Initialize: surfaces currency0 + currency1 as token_in/token_out
    so the pool registry can be built from canonical_events."""
    from eth_abi import encode
    from core.adapters.evm.decoders.generic import load_mapping_dir

    mappings = {m.protocol: m for m in load_mapping_dir(MAPPINGS_DIR)}
    init_event = next(e for e in mappings["uniswap_v4"].events if e.name == "Initialize")
    decoder = GenericABIDecoder(mappings["uniswap_v4"], init_event)
    expected_topic0 = (
        "0x"
        + __import__("eth_utils").keccak(
            text="Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)"
        ).hex()
    )
    assert decoder.topic0 == expected_topic0

    pool_id = "0x" + "12" * 32
    currency0 = "0x" + "11" * 20
    currency1 = "0x" + "22" * 20
    hooks = "0x" + "33" * 20

    data = "0x" + encode(
        ["uint24", "int24", "address", "uint160", "int24"],
        [3000, 60, hooks, 79228162514264337593543950336, 0],
    ).hex()
    log = {
        "address": "0x000000000004444c5dc75cb358380d2e3de08a90",
        "topics": [decoder.topic0, pool_id, _addr_topic(currency0), _addr_topic(currency1)],
        "data": data,
        "blockNumber": "0x1",
        "transactionHash": TX,
        "logIndex": "0x0",
    }
    decoded = decoder.decode(log, TS)
    assert decoded is not None
    assert decoded.event_type == "pool_create"
    assert decoded.token_in == currency0
    assert decoded.token_out == currency1
    assert decoded.extra["pool_id"] == pool_id
    assert decoded.extra["fee"] == "3000"
    assert decoded.extra["hooks"] == hooks
