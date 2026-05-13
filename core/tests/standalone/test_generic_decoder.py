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


def test_uniswap_v4_swap_decoder_with_pool_resolver():
    """UniV4 Swap: int128 amounts + PoolId-resolved tokens via the pool registry."""
    from eth_abi import encode
    from core.adapters.evm.decoders.generic import load_mapping_dir
    from core.adapters.evm.decoders import plugins as plugins_mod

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
    currency0 = "0x" + "11" * 20
    currency1 = "0x" + "22" * 20

    plugins_mod._reset_univ4_pool_resolver()
    plugins_mod._UNIV4_POOL_RESOLVER = lambda chain, pid: (
        (currency0, currency1) if pid == pool_id and chain == "ethereum" else None
    )
    plugins_mod._UNIV4_POOL_RESOLVER_LOADED = True

    try:
        # amount0 negative (user received currency0), amount1 positive (user paid currency1)
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
            "chain": "ethereum",
        }
        decoded = decoder.decode(log, TS)
        assert decoded is not None
        assert decoded.protocol == "uniswap_v4"
        assert decoded.event_type == "swap"
        assert decoded.entity_id == sender
        # User paid currency1, received currency0 (sign convention).
        assert decoded.token_in == currency1
        assert decoded.token_out == currency0
        assert decoded.amount_in == abs(a1)
        assert decoded.amount_out == abs(a0)
        assert decoded.extra["pool_id"] == pool_id
        assert decoded.extra["fee"] == str(fee)
    finally:
        plugins_mod._reset_univ4_pool_resolver()


def test_uniswap_v4_swap_unresolved_pool_leaves_tokens_none():
    """Swap on a pool not in the registry: amounts populated, tokens stay None."""
    from eth_abi import encode
    from core.adapters.evm.decoders.generic import load_mapping_dir
    from core.adapters.evm.decoders import plugins as plugins_mod

    mappings = {m.protocol: m for m in load_mapping_dir(MAPPINGS_DIR)}
    swap_event = next(e for e in mappings["uniswap_v4"].events if e.name == "Swap")
    decoder = GenericABIDecoder(mappings["uniswap_v4"], swap_event)

    pool_id = "0x" + "ab" * 32
    sender = "0x" + "cc" * 20

    plugins_mod._reset_univ4_pool_resolver()
    plugins_mod._UNIV4_POOL_RESOLVER = lambda chain, pid: None  # always-miss
    plugins_mod._UNIV4_POOL_RESOLVER_LOADED = True

    try:
        a0 = -(50 * 10**18)
        a1 = 25 * 10**6
        data = "0x" + encode(
            ["int128", "int128", "uint160", "uint128", "int24", "uint24"],
            [a0, a1, 79228162514264337593543950336, 1, 0, 500],
        ).hex()
        log = {
            "address": "0x000000000004444c5dc75cb358380d2e3de08a90",
            "topics": [decoder.topic0, pool_id, _addr_topic(sender)],
            "data": data,
            "blockNumber": "0x1",
            "transactionHash": TX,
            "logIndex": "0x0",
            "chain": "ethereum",
        }
        decoded = decoder.decode(log, TS)
        assert decoded is not None
        assert decoded.amount_in == abs(a1)
        assert decoded.amount_out == abs(a0)
        # Registry miss -> tokens unresolved
        assert decoded.token_in is None
        assert decoded.token_out is None
    finally:
        plugins_mod._reset_univ4_pool_resolver()


AAVE_V3_SUPPLY_TOPIC0   = "0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61"
AAVE_V3_WITHDRAW_TOPIC0 = "0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7"
AAVE_V3_BORROW_TOPIC0   = "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0"
AAVE_V3_REPAY_TOPIC0    = "0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051"

AAVE_POOL = "0x" + "ae" * 20
RESERVE   = "0x" + "11" * 20  # underlying asset (e.g. USDC)
USER      = "0x" + "22" * 20
ON_BEHALF = "0x" + "33" * 20
TO        = "0x" + "44" * 20
REPAYER   = "0x" + "55" * 20


def _uint16_topic(n: int) -> str:
    return "0x" + format(n, "064x")


def test_aave_v3_topic0s_match_known_hashes():
    for topic0 in (
        AAVE_V3_SUPPLY_TOPIC0,
        AAVE_V3_WITHDRAW_TOPIC0,
        AAVE_V3_BORROW_TOPIC0,
        AAVE_V3_REPAY_TOPIC0,
    ):
        assert _load_decoder("aave_v3", topic0).topic0 == topic0


def test_aave_v3_supply_decode():
    decoder = _load_decoder("aave_v3", AAVE_V3_SUPPLY_TOPIC0)
    amount = 5_000_000_000  # 5000 USDC (6 dec)
    data = encode(["address", "uint256"], [USER, amount]).hex()
    log = {
        "address": AAVE_POOL,
        "topics": [
            AAVE_V3_SUPPLY_TOPIC0,
            _addr_topic(RESERVE),
            _addr_topic(ON_BEHALF),
            _uint16_topic(42),  # referralCode
        ],
        "data": "0x" + data,
        "blockNumber": "0x100",
        "transactionHash": TX,
        "logIndex": "0x3",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.event_type == "lend_deposit"
    assert ev.protocol == "aave_v3"
    assert ev.entity_id == ON_BEHALF
    assert ev.venue == AAVE_POOL
    assert ev.token_in == RESERVE
    assert ev.amount_in == amount
    assert ev.extra["user"] == USER
    assert ev.extra["referral_code"] == "42"


def test_aave_v3_withdraw_decode():
    decoder = _load_decoder("aave_v3", AAVE_V3_WITHDRAW_TOPIC0)
    amount = 2_500_000_000
    data = encode(["uint256"], [amount]).hex()
    log = {
        "address": AAVE_POOL,
        "topics": [
            AAVE_V3_WITHDRAW_TOPIC0,
            _addr_topic(RESERVE),
            _addr_topic(USER),
            _addr_topic(TO),
        ],
        "data": "0x" + data,
        "blockNumber": "0x101",
        "transactionHash": TX,
        "logIndex": "0x4",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.event_type == "lend_withdraw"
    assert ev.entity_id == USER
    assert ev.token_out == RESERVE
    assert ev.amount_out == amount
    assert ev.extra["to"] == TO


def test_aave_v3_borrow_decode():
    decoder = _load_decoder("aave_v3", AAVE_V3_BORROW_TOPIC0)
    amount = 1_000_000_000
    interest_rate_mode = 2  # variable
    borrow_rate = 12345678
    data = encode(
        ["address", "uint256", "uint8", "uint256"],
        [USER, amount, interest_rate_mode, borrow_rate],
    ).hex()
    log = {
        "address": AAVE_POOL,
        "topics": [
            AAVE_V3_BORROW_TOPIC0,
            _addr_topic(RESERVE),
            _addr_topic(ON_BEHALF),
            _uint16_topic(0),
        ],
        "data": "0x" + data,
        "blockNumber": "0x102",
        "transactionHash": TX,
        "logIndex": "0x5",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.event_type == "lend_borrow"
    assert ev.entity_id == ON_BEHALF
    assert ev.token_out == RESERVE
    assert ev.amount_out == amount
    assert ev.extra["interest_rate_mode"] == "2"
    assert ev.extra["borrow_rate"] == str(borrow_rate)


def test_aave_v3_repay_decode():
    decoder = _load_decoder("aave_v3", AAVE_V3_REPAY_TOPIC0)
    amount = 750_000_000
    data = encode(["uint256", "bool"], [amount, True]).hex()
    log = {
        "address": AAVE_POOL,
        "topics": [
            AAVE_V3_REPAY_TOPIC0,
            _addr_topic(RESERVE),
            _addr_topic(USER),
            _addr_topic(REPAYER),
        ],
        "data": "0x" + data,
        "blockNumber": "0x103",
        "transactionHash": TX,
        "logIndex": "0x6",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.event_type == "lend_repay"
    assert ev.entity_id == REPAYER
    assert ev.token_in == RESERVE
    assert ev.amount_in == amount
    assert ev.extra["user"] == USER
    assert ev.extra["use_atokens"] == "True"


LIDO_SUBMITTED_TOPIC0 = "0x96a25c8ce0baabc1fdefd93e9ed25d8e092a3332f3aa9a41722b5697231d1d1a"
LIDO_WITHDRAWAL_REQUESTED_TOPIC0 = "0xf0cb471f23fb74ea44b8252eb1881a2dca546288d9f6e90d1a0e82fe0ed342ab"
LIDO_STETH = "0xae7ab96520de3a18e5e111b5eaab095312d7fe84"
LIDO_WITHDRAWAL_QUEUE = "0x889edc2edab5f40e902b864ad4d7ade8e412f9b1"


def _uint256_topic(n: int) -> str:
    return "0x" + format(n, "064x")


def test_lido_submitted_decode():
    decoder = _load_decoder("lido", LIDO_SUBMITTED_TOPIC0)
    amount = 32 * 10**18  # 32 ETH stake
    referral = "0x" + "77" * 20
    data = encode(["uint256", "address"], [amount, referral]).hex()
    log = {
        "address": LIDO_STETH,
        "topics": [LIDO_SUBMITTED_TOPIC0, _addr_topic(SENDER)],
        "data": "0x" + data,
        "blockNumber": "0x200",
        "transactionHash": TX,
        "logIndex": "0x10",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.event_type == "stake"
    assert ev.protocol == "lido"
    assert ev.entity_id == SENDER
    assert ev.venue == LIDO_STETH
    assert ev.token_in == "ETH"
    assert ev.amount_in == amount
    assert ev.extra["referral"] == referral


def test_lido_withdrawal_requested_decode():
    decoder = _load_decoder("lido", LIDO_WITHDRAWAL_REQUESTED_TOPIC0)
    request_id = 12345
    requestor = "0x" + "aa" * 20
    owner = "0x" + "bb" * 20
    amount_steth = 5 * 10**18
    amount_shares = int(4.8 * 10**18)
    data = encode(["uint256", "uint256"], [amount_steth, amount_shares]).hex()
    log = {
        "address": LIDO_WITHDRAWAL_QUEUE,
        "topics": [
            LIDO_WITHDRAWAL_REQUESTED_TOPIC0,
            _uint256_topic(request_id),
            _addr_topic(requestor),
            _addr_topic(owner),
        ],
        "data": "0x" + data,
        "blockNumber": "0x201",
        "transactionHash": TX,
        "logIndex": "0x11",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.event_type == "unstake"
    assert ev.protocol == "lido"
    assert ev.entity_id == owner
    assert ev.venue == LIDO_WITHDRAWAL_QUEUE
    assert ev.token_out == "stETH"
    assert ev.amount_out == amount_steth
    assert ev.extra["request_id"] == str(request_id)


COMPOUND_V3_SUPPLY_TOPIC0   = "0xd1cf3d156d5f8f0d50f6c122ed609cec09d35c9b9fb3fff6ea0959134dae424e"
COMPOUND_V3_WITHDRAW_TOPIC0 = "0x9b1bfa7fa9ee420a16e124f794c35ac9f90472acc99140eb2f6447c714cad8eb"
COMPOUND_USDC_COMET = "0xc3d688b66703497daa19211eedff47f25384cdc3"


def test_compound_v3_supply_decode():
    decoder = _load_decoder("compound_v3", COMPOUND_V3_SUPPLY_TOPIC0)
    amount = 1_000_000_000  # 1000 USDC
    src = "0x" + "11" * 20
    dst = "0x" + "22" * 20
    data = encode(["uint256"], [amount]).hex()
    log = {
        "address": COMPOUND_USDC_COMET,
        "topics": [
            COMPOUND_V3_SUPPLY_TOPIC0,
            _addr_topic(src),
            _addr_topic(dst),
        ],
        "data": "0x" + data,
        "blockNumber": "0x300",
        "transactionHash": TX,
        "logIndex": "0x20",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.event_type == "lend_deposit"
    assert ev.protocol == "compound_v3"
    assert ev.entity_id == dst
    assert ev.venue == COMPOUND_USDC_COMET
    assert ev.amount_in == amount
    assert ev.extra["from"] == src


def test_compound_v3_withdraw_decode():
    decoder = _load_decoder("compound_v3", COMPOUND_V3_WITHDRAW_TOPIC0)
    amount = 500_000_000
    src = "0x" + "33" * 20
    to = "0x" + "44" * 20
    data = encode(["uint256"], [amount]).hex()
    log = {
        "address": COMPOUND_USDC_COMET,
        "topics": [
            COMPOUND_V3_WITHDRAW_TOPIC0,
            _addr_topic(src),
            _addr_topic(to),
        ],
        "data": "0x" + data,
        "blockNumber": "0x301",
        "transactionHash": TX,
        "logIndex": "0x21",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.event_type == "lend_withdraw"
    assert ev.entity_id == src
    assert ev.amount_out == amount
    assert ev.extra["to"] == to


MORPHO_BLUE_SUPPLY_TOPIC0   = "0xedf8870433c83823eb071d3df1caa8d008f12f6440918c20d75a3602cda30fe0"
MORPHO_BLUE_BORROW_TOPIC0   = "0x570954540bed6b1304a87dfe815a5eda4a648f7097a16240dcd85c9b5fd42a43"
MORPHO_BLUE_SINGLETON = "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb"


def _bytes32_topic(b: bytes) -> str:
    assert len(b) == 32
    return "0x" + b.hex()


def test_morpho_blue_supply_decode():
    decoder = _load_decoder("morpho_blue", MORPHO_BLUE_SUPPLY_TOPIC0)
    market_id = b"\xab" * 32
    caller = "0x" + "11" * 20
    on_behalf = "0x" + "22" * 20
    assets = 1_000_000_000_000_000_000  # 1 ETH-ish
    shares = 950_000_000_000_000_000
    data = encode(["uint256", "uint256"], [assets, shares]).hex()
    log = {
        "address": MORPHO_BLUE_SINGLETON,
        "topics": [
            MORPHO_BLUE_SUPPLY_TOPIC0,
            _bytes32_topic(market_id),
            _addr_topic(caller),
            _addr_topic(on_behalf),
        ],
        "data": "0x" + data,
        "blockNumber": "0x400",
        "transactionHash": TX,
        "logIndex": "0x30",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.event_type == "lend_deposit"
    assert ev.protocol == "morpho_blue"
    assert ev.entity_id == on_behalf
    assert ev.amount_in == assets
    assert ev.extra["market_id"] == "0x" + market_id.hex()
    assert ev.extra["caller"] == caller


def test_morpho_blue_borrow_decode():
    decoder = _load_decoder("morpho_blue", MORPHO_BLUE_BORROW_TOPIC0)
    market_id = b"\xcd" * 32
    caller = "0x" + "33" * 20
    on_behalf = "0x" + "44" * 20
    receiver = "0x" + "55" * 20
    assets = 500_000_000  # 500 USDC
    shares = 495_000_000
    data = encode(["address", "uint256", "uint256"], [caller, assets, shares]).hex()
    log = {
        "address": MORPHO_BLUE_SINGLETON,
        "topics": [
            MORPHO_BLUE_BORROW_TOPIC0,
            _bytes32_topic(market_id),
            _addr_topic(on_behalf),
            _addr_topic(receiver),
        ],
        "data": "0x" + data,
        "blockNumber": "0x401",
        "transactionHash": TX,
        "logIndex": "0x31",
    }
    ev = decoder.decode(log, TS)
    assert ev is not None
    assert ev.event_type == "lend_borrow"
    assert ev.entity_id == on_behalf
    assert ev.amount_out == assets
    assert ev.extra["receiver"] == receiver


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
