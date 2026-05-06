"""Aggregator event decoders — CoW Protocol, 0x, 1inch.

These decoders identify aggregator-level swap events. Per ENGINEERING_PLAN.md
section 3.5, when an aggregator event exists in a transaction, underlying DEX
Swap events in the same tx are marked as 'swap_internal' and excluded from
trajectory queries by default.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from core.adapters.evm.decoders.base import DecodedEvent, LogDecoder


class CowTradeDecoder(LogDecoder):
    """CoW Protocol Trade event from GPv2Settlement.

    Emitted once per executed trade in a batch settlement.
    Contract: 0x9008D19f58AAbD9eD0D60971565AA8510560ab41 (Ethereum, Base, Arbitrum)
    """

    @property
    def topic0(self) -> str:
        return "0xa07a543ab8a018198e99ca0184c93fe9050a79400a0a723441f84de1d972cc17"

    @property
    def event_signature(self) -> str:
        return "Trade(address indexed owner, address sellToken, address buyToken, uint256 sellAmount, uint256 buyAmount, uint256 feeAmount, bytes orderUid)"

    @property
    def protocol(self) -> str:
        return "cowswap"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 2:
            return None
        owner = self._topic_address(topics[1])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(
            data,
            ["address", "address", "uint256", "uint256", "uint256", "bytes"],
        )
        sell_token = vals[0]
        buy_token = vals[1]
        sell_amount = Decimal(vals[2])
        buy_amount = Decimal(vals[3])
        fee_amount = Decimal(vals[4])

        return DecodedEvent(
            event_type="swap",
            entity_id=owner,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            token_in=sell_token,
            token_out=buy_token,
            amount_in=sell_amount,
            amount_out=buy_amount,
            aggregator="cowswap",
            extra={
                "sell_token": sell_token,
                "buy_token": buy_token,
                "sell_amount": str(sell_amount),
                "buy_amount": str(buy_amount),
                "fee_amount": str(fee_amount),
            },
        )


class ZeroExTransformedERC20Decoder(LogDecoder):
    """0x Exchange Proxy TransformedERC20 event.

    Emitted when the ExchangeProxy executes an ERC20 transform (the internal
    mechanism for filling orders).  This is the closest thing to a swap event
    that 0x emits, as RfqOrderFilled and LimitOrderFilled are emitted by
    the ExchangeProxy but may use different topic0s depending on version.

    Contract: 0xDef1C0ded9bec7F1a1670819833240f027b25EfF (all chains)
    """

    @property
    def topic0(self) -> str:
        return "0x0f6672f78a59ba8e5e5b5d38df3ebc67f3c792e2c9259b8d97d7f00dd78ba1b3"

    @property
    def event_signature(self) -> str:
        return "TransformedERC20(address indexed taker, address inputToken, address outputToken, uint256 inputAmount, uint256 outputAmount)"

    @property
    def protocol(self) -> str:
        return "0x"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 2:
            return None
        taker = self._topic_address(topics[1])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(
            data,
            ["address", "address", "uint256", "uint256"],
        )
        input_token = vals[0]
        output_token = vals[1]
        input_amount = Decimal(vals[2])
        output_amount = Decimal(vals[3])

        return DecodedEvent(
            event_type="swap",
            entity_id=taker,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            token_in=input_token,
            token_out=output_token,
            amount_in=input_amount,
            amount_out=output_amount,
            aggregator="0x",
            extra={
                "input_token": input_token,
                "output_token": output_token,
                "input_amount": str(input_amount),
                "output_amount": str(output_amount),
            },
        )


class OneInchOrderFilledDecoder(LogDecoder):
    """1inch AggregationRouterV5 OrderFilled event.

    Emitted by the 1inch limit order protocol when an order is filled.
    On mainnet this is sparse (~2 events/day); most 1inch usage goes
    through the swap() function which doesn't emit its own swap event.

    Contract: 0x1111111254eeb25477b68fb85ed929f73a960582 (Ethereum)
    """

    @property
    def topic0(self) -> str:
        return "0xb9ed0243fdf00f0545c63a0af8850c090d86bb46682baec4bf3c496814fe4f02"

    @property
    def event_signature(self) -> str:
        return "OrderFilled(address indexed maker, bytes32 orderHash, uint256 remaining)"

    @property
    def protocol(self) -> str:
        return "1inch"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 2:
            return None
        maker = self._topic_address(topics[1])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(data, ["bytes32", "uint256"])
        order_hash = "0x" + vals[0].hex() if isinstance(vals[0], bytes) else str(vals[0])
        remaining = Decimal(vals[1])

        return DecodedEvent(
            event_type="swap",
            entity_id=maker,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            aggregator="1inch",
            extra={
                "order_hash": order_hash,
                "remaining": str(remaining),
            },
        )
