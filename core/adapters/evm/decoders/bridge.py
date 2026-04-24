"""Bridge event decoders."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from core.adapters.evm.decoders.base import DecodedEvent, LogDecoder


class StargateSendToChainDecoder(LogDecoder):
    @property
    def topic0(self) -> str:
        return "0x664e26797cde1146ddfcb9a5d3f4de61179f9c11b2698599bb09e686f442172b"

    @property
    def event_signature(self) -> str:
        return "SendToChain(uint16 dstChainId, bytes to, uint256 qty)"

    @property
    def protocol(self) -> str:
        return "stargate"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(data, ["uint16", "bytes", "uint256"])
        dst_chain_id = vals[0]
        to_raw = vals[1]
        qty = Decimal(vals[2])
        to_addr = "0x" + to_raw.hex()[-40:] if len(to_raw) >= 20 else ""

        return DecodedEvent(
            event_type="bridge_out",
            entity_id=log["address"],
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            amount_out=qty,
            link_key=str(dst_chain_id),
            link_key_type="stargate_dst_chain",
            extra={"dst_chain_id": dst_chain_id, "to": to_addr, "qty": str(qty)},
        )


class AcrossV3FundsDepositedDecoder(LogDecoder):
    @property
    def topic0(self) -> str:
        return "0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f"

    @property
    def event_signature(self) -> str:
        return "V3FundsDeposited(address inputToken, address outputToken, uint256 inputAmount, uint256 outputAmount, uint256 indexed destinationChainId, uint32 indexed depositId, uint32 quoteTimestamp, uint32 fillDeadline, uint32 exclusivityDeadline, address indexed depositor, address recipient, address exclusiveRelayer, bytes message)"

    @property
    def protocol(self) -> str:
        return "across"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 4:
            return None
        destination_chain_id = int(topics[1], 16)
        deposit_id = int(topics[2], 16)
        depositor = self._topic_address(topics[3])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(
            data,
            [
                "address",
                "address",
                "uint256",
                "uint256",
                "uint32",
                "uint32",
                "uint32",
                "address",
                "address",
                "address",
                "bytes",
            ],
        )
        input_token = vals[0]
        output_token = vals[1]
        input_amount = Decimal(vals[2])
        output_amount = Decimal(vals[3])

        return DecodedEvent(
            event_type="bridge_out",
            entity_id=depositor,
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
            link_key=str(deposit_id),
            link_key_type="across_deposit_id",
            extra={
                "destination_chain_id": destination_chain_id,
                "deposit_id": deposit_id,
                "input_token": input_token,
                "output_token": output_token,
            },
        )


class BaseETHBridgeInitiatedDecoder(LogDecoder):
    @property
    def topic0(self) -> str:
        return "0x2849b43074093a05396b6f2a937dee8565b15a48a7b3d4bffb732a5017380af5"

    @property
    def event_signature(self) -> str:
        return "ETHBridgeInitiated(address indexed from, address indexed to, uint256 amount, bytes extraData)"

    @property
    def protocol(self) -> str:
        return "base_native"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 3:
            return None
        sender = self._topic_address(topics[1])
        receiver = self._topic_address(topics[2])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(data, ["uint256", "bytes"])
        amount = Decimal(vals[0])

        return DecodedEvent(
            event_type="bridge_out",
            entity_id=sender,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            token_out="ETH",
            amount_out=amount,
            counterparty=receiver,
            link_key=f"{log['transactionHash']}:eth",
            link_key_type="base_bridge_tx",
            extra={"receiver": receiver, "amount": str(amount)},
        )


class BaseERC20BridgeInitiatedDecoder(LogDecoder):
    @property
    def topic0(self) -> str:
        return "0x7ff126db8024424bbfd9826e8ab82ff59136289ea440b04b39a0df1b03b9cabf"

    @property
    def event_signature(self) -> str:
        return "ERC20BridgeInitiated(address indexed localToken, address indexed remoteToken, address indexed from, address to, uint256 amount, bytes extraData)"

    @property
    def protocol(self) -> str:
        return "base_native"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 4:
            return None
        local_token = self._topic_address(topics[1])
        remote_token = self._topic_address(topics[2])
        sender = self._topic_address(topics[3])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(data, ["address", "uint256", "bytes"])
        receiver = vals[0]
        amount = Decimal(vals[1])

        return DecodedEvent(
            event_type="bridge_out",
            entity_id=sender,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            token_out=local_token,
            amount_out=amount,
            counterparty=receiver,
            link_key=f"{log['transactionHash']}:erc20",
            link_key_type="base_bridge_tx",
            extra={"remote_token": remote_token, "receiver": receiver, "amount": str(amount)},
        )
