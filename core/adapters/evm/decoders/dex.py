"""DEX swap decoders: Uniswap V2, V3, and forks."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from core.adapters.evm.decoders.base import DecodedEvent, LogDecoder


class UniswapV2SwapDecoder(LogDecoder):
    @property
    def topic0(self) -> str:
        return "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"

    @property
    def event_signature(self) -> str:
        return "Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)"

    @property
    def protocol(self) -> str:
        return "uniswap_v2"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 3:
            return None
        sender = self._topic_address(topics[1])
        to = self._topic_address(topics[2])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(data, ["uint256", "uint256", "uint256", "uint256"])
        amount0_in, amount1_in, amount0_out, amount1_out = (Decimal(v) for v in vals)

        return DecodedEvent(
            event_type="swap",
            entity_id=sender,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            amount_in=amount0_in if amount0_in > 0 else amount1_in,
            amount_out=amount0_out if amount0_out > 0 else amount1_out,
            extra={
                "amount0_in": str(amount0_in),
                "amount1_in": str(amount1_in),
                "amount0_out": str(amount0_out),
                "amount1_out": str(amount1_out),
                "to": to,
            },
        )


class UniswapV3SwapDecoder(LogDecoder):
    @property
    def topic0(self) -> str:
        return "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

    @property
    def event_signature(self) -> str:
        return "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"

    @property
    def protocol(self) -> str:
        return "uniswap_v3"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 3:
            return None
        sender = self._topic_address(topics[1])
        recipient = self._topic_address(topics[2])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(data, ["int256", "int256", "uint160", "uint128", "int24"])
        amount0, amount1 = Decimal(vals[0]), Decimal(vals[1])

        return DecodedEvent(
            event_type="swap",
            entity_id=sender,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            amount_in=amount0 if amount0 > 0 else amount1,
            amount_out=abs(amount0) if amount0 < 0 else abs(amount1),
            extra={
                "amount0": str(amount0),
                "amount1": str(amount1),
                "recipient": recipient,
            },
        )
