"""Chain identifier mappings for cross-chain normalization.

Sources:
  - LayerZero V2 endpoint IDs: layerzero.network
  - EVM chain IDs: chainlist.org
  - Spellbook bridges_layerzero_chain_indexes.sql (V1 IDs for reference)
"""

# LayerZero V2 endpoint ID → chain name
EID_TO_CHAIN: dict[int, str] = {
    30101: "ethereum",
    30110: "arbitrum",
    30111: "optimism",
    30184: "base",
    30109: "polygon",
    30106: "avalanche_c",
    30102: "bnb",
    30125: "celo",
    30165: "zksync",
    30145: "gnosis",
    30214: "scroll",
    30243: "blast",
    30260: "mode",
    30290: "taiko",
    30274: "xlayer",
    30181: "mantle",
    30203: "opbnb",
    30175: "nova",
}

# EVM chain ID → chain name
CHAIN_ID_TO_CHAIN: dict[int, str] = {
    1: "ethereum",
    10: "optimism",
    42161: "arbitrum",
    8453: "base",
    137: "polygon",
    43114: "avalanche_c",
    56: "bnb",
    42220: "celo",
    324: "zksync",
    100: "gnosis",
    534352: "scroll",
    81457: "blast",
    34443: "mode",
    59144: "linea",
    7777777: "zora",
    204: "opbnb",
}

# Both directions for convenience
CHAIN_ID_TO_CHAIN.update({
    # Allow string keys too (from decoded event data)
    str(k): v for k, v in CHAIN_ID_TO_CHAIN.items()
})


def normalize_chain(key: str, key_type: str) -> str | None:
    """Normalize a bridge link_key to a chain name.

    Stargate bridge_out uses dst_chain_id (EVM chain ID like 8453).
    Stargate bridge_in uses src_eid (LayerZero endpoint ID like 30184).
    Both resolve to the same chain name (e.g. "base") for matching.
    """
    try:
        num = int(key)
    except (ValueError, TypeError):
        return None

    if key_type in ("stargate_dst_chain",):
        return CHAIN_ID_TO_CHAIN.get(num)
    elif key_type in ("stargate_src_eid", "layerzero_src_eid"):
        return EID_TO_CHAIN.get(num)
    return None
