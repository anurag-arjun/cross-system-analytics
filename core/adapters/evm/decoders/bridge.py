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


class AcrossFundsDepositedDecoder(LogDecoder):
    """Across V3 FundsDeposited — bridge_out event on source chain.

    Note: V3FundsDeposited is a LEGACY event (unused, kept for ABI migration).
    The active deposit event is FundsDeposited which uses bytes32 for addresses.
    Verified against V3SpokePoolInterface.sol from across-protocol/contracts."""

    @property
    def topic0(self) -> str:
        return "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3"

    @property
    def event_signature(self) -> str:
        return "FundsDeposited(bytes32 inputToken, bytes32 outputToken, uint256 inputAmount, uint256 outputAmount, uint256 indexed destinationChainId, uint256 indexed depositId, uint32 quoteTimestamp, uint32 fillDeadline, uint32 exclusivityDeadline, bytes32 indexed depositor, bytes32 recipient, bytes32 exclusiveRelayer, bytes message)"

    @property
    def protocol(self) -> str:
        return "across"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 4:
            return None
        destination_chain_id = int(topics[1], 16)
        deposit_id = int(topics[2], 16)
        depositor = self._bytes32_to_address(topics[3])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(
            data,
            [
                "bytes32",  # inputToken
                "bytes32",  # outputToken
                "uint256",  # inputAmount
                "uint256",  # outputAmount
                "uint32",   # quoteTimestamp
                "uint32",   # fillDeadline
                "uint32",   # exclusivityDeadline
                "bytes32",  # recipient
                "bytes32",  # exclusiveRelayer
                "bytes",    # message
            ],
        )
        input_token = self._bytes32_to_address(vals[0])
        output_token = self._bytes32_to_address(vals[1])
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

    def _bytes32_to_address(self, val: str | bytes) -> str:
        """Convert a bytes32 value to an Ethereum address (last 20 bytes)."""
        if isinstance(val, str):
            val = val.lower().replace("0x", "")
            return "0x" + val[-40:]
        if isinstance(val, bytes):
            return "0x" + val.hex()[-40:]
        return str(val)[-42:]


class BaseETHBridgeInitiatedDecoder(LogDecoder):
    @property
    def topic0(self) -> str:
        return "0x2849b43074093a05396b6f2a937dee8565b15a48a7b3d4bffb732a5017380af5"

    @property
    def event_signature(self) -> str:
        return "ETHBridgeInitiated(address indexed from, address indexed to, uint256 amount, bytes extraData)"

    @property
    def protocol(self) -> str:
        return "op_stack"

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
            link_key_type="op_stack_bridge",
            extra={"receiver": receiver, "amount": str(amount)},
        )


class AcrossFilledRelayDecoder(LogDecoder):
    """Across V3 FilledRelay — bridge_in event on destination chain.

    Note: FilledV3Relay is a LEGACY event. The active fill event is FilledRelay
    which uses bytes32 for addresses and has different parameter types."""

    @property
    def topic0(self) -> str:
        return "0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208"

    @property
    def event_signature(self) -> str:
        return "FilledRelay(bytes32 inputToken, bytes32 outputToken, uint256 inputAmount, uint256 outputAmount, uint256 repaymentChainId, uint256 indexed originChainId, uint256 indexed depositId, uint32 fillDeadline, uint32 exclusivityDeadline, bytes32 exclusiveRelayer, bytes32 indexed relayer, bytes32 depositor, bytes32 recipient, bytes32 messageHash, (bytes32,bytes32,uint256,uint8) relayExecutionInfo)"

    @property
    def protocol(self) -> str:
        return "across"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 4:
            return None
        origin_chain_id = int(topics[1], 16)
        deposit_id = int(topics[2], 16)
        relayer = self._bytes32_to_address(topics[3])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(
            data,
            [
                "bytes32",  # inputToken
                "bytes32",  # outputToken
                "uint256",  # inputAmount
                "uint256",  # outputAmount
                "uint256",  # repaymentChainId
                "uint32",   # fillDeadline
                "uint32",   # exclusivityDeadline
                "bytes32",  # exclusiveRelayer
                "bytes32",  # depositor
                "bytes32",  # recipient
                "bytes32",  # messageHash
                "(bytes32,bytes32,uint256,uint8)",  # relayExecutionInfo
            ],
        )
        input_token = self._bytes32_to_address(vals[0])
        output_token = self._bytes32_to_address(vals[1])
        input_amount = Decimal(vals[2])
        output_amount = Decimal(vals[3])
        # repaymentChainId = vals[4]
        depositor = self._bytes32_to_address(vals[8])
        recipient = self._bytes32_to_address(vals[9])

        return DecodedEvent(
            event_type="bridge_in",
            entity_id=recipient,
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
            counterparty=relayer,
            link_key=str(deposit_id),
            link_key_type="across_deposit_id",
            extra={
                "origin_chain_id": origin_chain_id,
                "deposit_id": deposit_id,
                "depositor": depositor,
                "recipient": recipient,
                "relayer": relayer,
            },
        )

    def _bytes32_to_address(self, val: str | bytes) -> str:
        """Convert a bytes32 value to an Ethereum address (last 20 bytes)."""
        if isinstance(val, str):
            val = val.lower().replace("0x", "")
            return "0x" + val[-40:]
        if isinstance(val, bytes):
            return "0x" + val.hex()[-40:]
        return str(val)[-42:]


class StargateReceiveFromChainDecoder(LogDecoder):
    """Stargate v2 ReceiveFromChain — bridge-in event on destination chain."""

    @property
    def topic0(self) -> str:
        return "0x3f25d151146756967e776269c39767851a59ad8c562b8f74083801d8b0e3a2ac"

    @property
    def event_signature(self) -> str:
        return "ReceiveFromChain(uint16 srcEid, uint256 sender, address receiver, uint256 amount, bytes message)"

    @property
    def protocol(self) -> str:
        return "stargate"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 1:
            return None
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(data, ["uint16", "uint256", "address", "uint256", "bytes"])
        src_eid = vals[0]
        sender = vals[1]
        receiver = vals[2]
        amount = Decimal(vals[3])

        return DecodedEvent(
            event_type="bridge_in",
            entity_id=receiver,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            amount_in=amount,
            counterparty=sender,
            link_key=str(src_eid),
            link_key_type="stargate_src_eid",
            extra={
                "src_eid": src_eid,
                "sender": str(sender),
                "receiver": receiver,
                "amount": str(amount),
            },
        )


class ETHBridgeFinalizedDecoder(LogDecoder):
    """ETHBridgeFinalized — bridge_in on L2 (L1→L2 deposit completed) or
    L1 (L2→L1 withdrawal completed).  Emitted by both L1StandardBridge and
    L2StandardBridge.

    Link key: tx_hash:eth for now (heuristic).  Precise matching via the
    CrossDomainMessenger's RelayedMessage msgHash will be added per ADR-002."""

    @property
    def topic0(self) -> str:
        return "0x31b2166ff604fc5672ea5df08a78081d2bc6d746cadce880747f3643d819e83d"

    @property
    def event_signature(self) -> str:
        return "ETHBridgeFinalized(address indexed from, address indexed to, uint256 amount, bytes extraData)"

    @property
    def protocol(self) -> str:
        return "op_stack"

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
            event_type="bridge_in",
            entity_id=receiver,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            token_in="ETH",
            amount_in=amount,
            counterparty=sender,
            link_key=f"{log['transactionHash']}:eth",
            link_key_type="op_stack_bridge",
            extra={"from": sender, "receiver": receiver, "amount": str(amount)},
        )


class ERC20BridgeFinalizedDecoder(LogDecoder):
    """ERC20BridgeFinalized — bridge_in on L2 (L1→L2 deposit completed) or
    L1 (L2→L1 withdrawal completed).  Emitted by both L1StandardBridge and
    L2StandardBridge."""

    @property
    def topic0(self) -> str:
        return "0xd59c65b35445225835c83f50b6ede06a7be047d22e357073e250d9af537518cd"

    @property
    def event_signature(self) -> str:
        return "ERC20BridgeFinalized(address indexed localToken, address indexed remoteToken, address indexed from, address to, uint256 amount, bytes extraData)"

    @property
    def protocol(self) -> str:
        return "op_stack"

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
            event_type="bridge_in",
            entity_id=receiver,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            token_in=local_token,
            amount_in=amount,
            counterparty=sender,
            link_key=f"{log['transactionHash']}:erc20",
            link_key_type="op_stack_bridge",
            extra={
                "local_token": local_token,
                "remote_token": remote_token,
                "from": sender,
                "receiver": receiver,
                "amount": str(amount),
            },
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
        return "op_stack"

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
            link_key_type="op_stack_bridge",
            extra={"remote_token": remote_token, "receiver": receiver, "amount": str(amount)},
        )
