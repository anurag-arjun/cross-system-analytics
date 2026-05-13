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
    """DEPRECATED: Stargate's actual bridge_in is LayerZero PacketDelivered.

    The ReceiveFromChain event signature is unverified — zero events found
    on any chain.  Use LayerZeroPacketDeliveredDecoder instead.
    Kept for reference until migration is complete."""

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


class LayerZeroPacketDeliveredDecoder(LogDecoder):
    """LayerZero V2 PacketDelivered — bridge_in for all LZ-based bridges.

    Emitted by the EndpointV2 contract when a cross-chain message is
    delivered.  This is the canonical bridge_in event for Stargate, OFT,
    and any protocol built on LayerZero.

    The Origin struct contains srcEid (source chain's endpoint ID),
    which we normalize to a chain name for bridge matching with Stargate's
    SendToChain (which uses EVM chain IDs).

    Verified on-chain: 3,580 Ethereum, 95 Arbitrum events."""

    @property
    def topic0(self) -> str:
        return "0x3cd5e48f9730b129dc7550f0fcea9c767b7be37837cd10e55eb35f734f4bca04"

    @property
    def event_signature(self) -> str:
        return "PacketDelivered((uint32 srcEid, bytes32 sender, uint64 nonce), address receiver)"

    @property
    def protocol(self) -> str:
        return "layerzero"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        data = log.get("data", "0x")
        if data == "0x":
            return None
        # Origin struct: (uint32 srcEid, bytes32 sender, uint64 nonce)
        vals = self._decode_abi(data, ["(uint32,bytes32,uint64)", "address"])
        origin = vals[0]  # tuple (srcEid, sender, nonce)
        receiver = vals[1]
        src_eid = origin[0]
        sender_bytes32 = origin[1]
        nonce = origin[2]
        sender = self._address_from_bytes32(sender_bytes32)

        return DecodedEvent(
            event_type="bridge_in",
            entity_id=receiver,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            counterparty=sender,
            # The bridge engine composes the full composite link_key
            # in _composite_link_key — needs (src_eid, sender, nonce,
            # dst_eid) where dst_eid is the chain we're decoding for.
            link_key=str(src_eid),
            link_key_type="layerzero_src_eid",
            extra={
                "src_eid": src_eid,
                "sender": sender,
                "nonce": nonce,
                "receiver": receiver,
            },
        )

    def _address_from_bytes32(self, val: str | bytes) -> str:
        """Convert a bytes32 value to an Ethereum address."""
        if isinstance(val, str):
            return "0x" + val.lower().replace("0x", "")[-40:]
        if isinstance(val, bytes):
            return "0x" + val.hex()[-40:]
        return str(val)[-42:]


class LayerZeroPacketSentDecoder(LogDecoder):
    """LayerZero V2 PacketSent — bridge_out for all LZ-based bridges.

    Emitted by the EndpointV2 contract on the source chain when a
    cross-chain message is sent. The companion of PacketDelivered.

    Event: PacketSent(bytes encodedPayload, bytes options, address sendLibrary)

    encodedPayload is the LZ V1 packet codec (PacketV1Codec.sol):
        byte    0    : packet version (uint8, =1)
        bytes 1-8    : nonce (uint64, big-endian)
        bytes 9-12   : srcEid (uint32, big-endian)
        bytes 13-44  : sender (bytes32 — left-padded address)
        bytes 45-48  : dstEid (uint32, big-endian)
        bytes 49-80  : receiver (bytes32 — chain-native; left-padded address on EVM)
        bytes 81-112 : guid (bytes32)
        bytes 113+   : application message (variable)

    link_key composition is deferred to `_composite_link_key`; the
    decoder seeds src_eid (raw), nonce, sender, dst_eid via extra and
    sets link_key=str(src_eid) for symmetry with PacketDelivered.
    """

    @property
    def topic0(self) -> str:
        return "0x1ab700d4ced0c005b164c0f789fd09fcbb0156d4c2041b8a3bfbcd961cd1567f"

    @property
    def event_signature(self) -> str:
        return "PacketSent(bytes encodedPayload, bytes options, address sendLibrary)"

    @property
    def protocol(self) -> str:
        return "layerzero"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(data, ["bytes", "bytes", "address"])
        encoded_payload = vals[0]
        if not isinstance(encoded_payload, (bytes, bytearray)) or len(encoded_payload) < 113:
            return None

        # version = encoded_payload[0]  # always 1 for V2; unused
        nonce = int.from_bytes(encoded_payload[1:9], "big")
        src_eid = int.from_bytes(encoded_payload[9:13], "big")
        sender_b32 = encoded_payload[13:45]
        dst_eid = int.from_bytes(encoded_payload[45:49], "big")
        receiver_b32 = encoded_payload[49:81]
        guid = encoded_payload[81:113]

        sender = "0x" + sender_b32.hex()[-40:]
        receiver = "0x" + receiver_b32.hex()[-40:]

        return DecodedEvent(
            event_type="bridge_out",
            entity_id=sender,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            counterparty=receiver,
            link_key=str(src_eid),
            link_key_type="layerzero_src_eid",
            extra={
                "src_eid": src_eid,
                "dst_eid": dst_eid,
                "nonce": nonce,
                "sender": sender,
                "receiver": receiver,
                "guid": "0x" + guid.hex(),
            },
        )


class OPMessagePassedDecoder(LogDecoder):
    """OP Stack L2→L1 withdrawal initiated (bridge_out).

    Emitted by L2ToL1MessagePasser (0x42...0016) on L2 when a withdrawal
    is initiated.  The withdrawalHash is the precise cross-chain key used
    for matching with WithdrawalProven/WithdrawalFinalized on L1 (ADR-002).
    """

    @property
    def topic0(self) -> str:
        return "0x02a52367d10742d8032712c1bb8e0144ff1ec5ffda1ed7d70bb05a2744955054"

    @property
    def event_signature(self) -> str:
        return "MessagePassed(uint256 indexed nonce, address indexed sender, address indexed target, uint256 value, uint256 gasLimit, bytes data, bytes32 withdrawalHash)"

    @property
    def protocol(self) -> str:
        return "op_stack"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 4:
            return None
        sender = self._topic_address(topics[2])
        target = self._topic_address(topics[3])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(data, ["uint256", "uint256", "bytes", "bytes32"])
        value = Decimal(vals[0])
        withdrawal_hash = "0x" + vals[3].hex() if isinstance(vals[3], bytes) else str(vals[3])

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
            amount_out=value,
            counterparty=target,
            link_key=withdrawal_hash,
            link_key_type="op_withdrawal_hash",
            extra={"withdrawal_hash": withdrawal_hash, "target": target, "value": str(value)},
        )


class OPWithdrawalProvenDecoder(LogDecoder):
    """OP Stack WithdrawalProven on L1 (bridge_in signal).

    Emitted by OptimismPortal on Ethereum when a withdrawal's fault proof
    has been submitted.
    """

    @property
    def topic0(self) -> str:
        return "0x67a6208cfcc0801d50f6cbe764733f4fddf66ac0b04442061a8a8c0cb6b63f62"

    @property
    def event_signature(self) -> str:
        return "WithdrawalProven(bytes32 indexed withdrawalHash, address indexed from, address indexed to)"

    @property
    def protocol(self) -> str:
        return "op_stack"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 4:
            return None
        withdrawal_hash = topics[1]
        sender = self._topic_address(topics[2])
        target = self._topic_address(topics[3])

        return DecodedEvent(
            event_type="bridge_in",
            entity_id=sender,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            counterparty=target,
            link_key=withdrawal_hash,
            link_key_type="op_withdrawal_hash",
            extra={"withdrawal_hash": withdrawal_hash, "from": sender, "to": target},
        )


class ArbitrumDepositInitiatedDecoder(LogDecoder):
    """Arbitrum DepositInitiated — L1→L2 bridge_out on Ethereum.

    Emitted by L1ERC20Gateway on Ethereum when a user initiates a deposit
    from L1 to Arbitrum.  Contains sequenceNumber for bridge matching.

    Per BRIDGE_RESEARCH.md: Spellbook uses gateway events, not Inbox/Outbox."""

    @property
    def topic0(self) -> str:
        return "0x135826c80512d99f4b9b54847a22d329fe0303e24e975f7d328ccb9cd80e3154"

    @property
    def event_signature(self) -> str:
        return "DepositInitiated(address indexed l1Token, address indexed l2Token, address indexed from, address to, uint256 sequenceNumber, uint256 amount)"

    @property
    def protocol(self) -> str:
        return "arbitrum_bridge"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 4:
            return None
        l1_token = self._topic_address(topics[1])
        l2_token = self._topic_address(topics[2])
        sender = self._topic_address(topics[3])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(data, ["address", "uint256", "uint256"])
        receiver = vals[0]
        sequence_number = vals[1]
        amount = Decimal(vals[2])

        return DecodedEvent(
            event_type="bridge_out",
            entity_id=sender,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            token_in=l1_token,
            token_out=l2_token,
            amount_out=amount,
            counterparty=receiver,
            link_key=str(sequence_number),
            link_key_type="arbitrum_sequence",
            extra={
                "l1_token": l1_token,
                "l2_token": l2_token,
                "from": sender,
                "to": receiver,
                "sequence_number": str(sequence_number),
                "amount": str(amount),
            },
        )


class ArbitrumWithdrawalFinalizedDecoder(LogDecoder):
    """Arbitrum WithdrawalFinalized — L2→L1 bridge_in on Ethereum.

    Emitted by L1ERC20Gateway on Ethereum when an Arbitrum→L1 withdrawal
    is finalized.  Contains exitNum which matches the L2 withdrawal.

    Per BRIDGE_RESEARCH.md: Spellbook uses gateway events for matching."""

    @property
    def topic0(self) -> str:
        return "0x217d412c201ebee3ff118753c0fda21adba4ae0b3a022f116e9e2508022b384b"

    @property
    def event_signature(self) -> str:
        return "WithdrawalFinalized(address indexed l1Token, address indexed l2Token, address indexed from, address to, uint256 exitNum, uint256 amount)"

    @property
    def protocol(self) -> str:
        return "arbitrum_bridge"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 4:
            return None
        l1_token = self._topic_address(topics[1])
        l2_token = self._topic_address(topics[2])
        sender = self._topic_address(topics[3])
        data = log.get("data", "0x")
        if data == "0x":
            return None
        vals = self._decode_abi(data, ["address", "uint256", "uint256"])
        receiver = vals[0]
        exit_num = vals[1]
        amount = Decimal(vals[2])

        return DecodedEvent(
            event_type="bridge_in",
            entity_id=receiver,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            token_in=l2_token,
            token_out=l1_token,
            amount_in=amount,
            counterparty=sender,
            link_key=str(exit_num),
            link_key_type="arbitrum_exit_num",
            extra={
                "l1_token": l1_token,
                "l2_token": l2_token,
                "from": sender,
                "to": receiver,
                "exit_num": str(exit_num),
                "amount": str(amount),
            },
        )


class ArbitrumOutBoxTransactionExecutedDecoder(LogDecoder):
    """Arbitrum OutBoxTransactionExecuted — L2→L1 execution confirmation.

    Emitted by ArbOutbox on Ethereum when a L2→L1 transaction is executed
    after the 7-day challenge period.  No token amounts (use
    ArbitrumWithdrawalFinalizedDecoder for amounts).

    Provides the L2 transaction index for precise matching."""

    @property
    def topic0(self) -> str:
        return "0x20af7f3bbfe38132b8900ae295cd9c8d1914be7052d061a511f3f728dab18964"

    @property
    def event_signature(self) -> str:
        return "OutBoxTransactionExecuted(address indexed to, address indexed l2Sender, uint256 indexed index, uint256 txNum)"

    @property
    def protocol(self) -> str:
        return "arbitrum_bridge"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 4:
            return None
        to_addr = self._topic_address(topics[1])
        l2_sender = self._topic_address(topics[2])
        # topics[3] may be int, bytes, or hex string depending on client
        idx = topics[3]
        if isinstance(idx, int):
            index = idx
        elif isinstance(idx, bytes):
            index = int.from_bytes(idx, 'big')
        else:
            index = int(str(idx), 16)
        data = log.get("data", "0x")
        tx_num = 0
        if data != "0x":
            vals = self._decode_abi(data, ["uint256"])
            tx_num = vals[0]

        return DecodedEvent(
            event_type="bridge_in",
            entity_id=to_addr,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            counterparty=l2_sender,
            link_key=str(index),
            link_key_type="arbitrum_outbox_index",
            extra={
                "to": to_addr,
                "l2_sender": l2_sender,
                "index": str(index),
                "tx_num": str(tx_num),
            },
        )


class OPWithdrawalFinalizedDecoder(LogDecoder):
    """OP Stack WithdrawalFinalized on L1 (bridge_in final confirmation).

    Emitted by OptimismPortal on Ethereum after the 7-day challenge period."""

    @property
    def topic0(self) -> str:
        return "0xdb5c7652857aa163daadd670e116628fb42e869d8ac4251ef8971d9e5727df1b"

    @property
    def event_signature(self) -> str:
        return "WithdrawalFinalized(bytes32 indexed withdrawalHash, bool success)"

    @property
    def protocol(self) -> str:
        return "op_stack"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 2:
            return None
        withdrawal_hash = topics[1]
        data = log.get("data", "0x")
        success = False
        if data != "0x":
            vals = self._decode_abi(data, ["bool"])
            success = vals[0]

        return DecodedEvent(
            event_type="bridge_in",
            entity_id="",
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            link_key=withdrawal_hash,
            link_key_type="op_withdrawal_hash",
            extra={"withdrawal_hash": withdrawal_hash, "success": success},
        )
