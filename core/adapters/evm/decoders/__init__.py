from core.adapters.evm.decoders.aggregator import (
    CowTradeDecoder,
    OneInchOrderFilledDecoder,
    ZeroExTransformedERC20Decoder,
)
from core.adapters.evm.decoders.base import DecodedEvent, LogDecoder
from core.adapters.evm.decoders.bridge import (
    AcrossFilledRelayDecoder,
    AcrossFundsDepositedDecoder,
    ArbitrumDepositInitiatedDecoder,
    ArbitrumOutBoxTransactionExecutedDecoder,
    ArbitrumWithdrawalFinalizedDecoder,
    BaseERC20BridgeInitiatedDecoder,
    BaseETHBridgeInitiatedDecoder,
    ERC20BridgeFinalizedDecoder,
    ETHBridgeFinalizedDecoder,
    LayerZeroPacketDeliveredDecoder,
    LayerZeroPacketSentDecoder,
    OPMessagePassedDecoder,
    OPWithdrawalFinalizedDecoder,
    OPWithdrawalProvenDecoder,
    StargateReceiveFromChainDecoder,
    StargateSendToChainDecoder,
)
from core.adapters.evm.decoders.token import (
    ApprovalDecoder,
    TransferDecoder,
    WETHDepositDecoder,
    WETHWithdrawalDecoder,
)

__all__ = [
    "DecodedEvent",
    "LogDecoder",
    "TransferDecoder",
    "ApprovalDecoder",
    "WETHDepositDecoder",
    "WETHWithdrawalDecoder",
    "StargateSendToChainDecoder",
    "StargateReceiveFromChainDecoder",
    "AcrossFundsDepositedDecoder",
    "AcrossFilledRelayDecoder",
    "BaseETHBridgeInitiatedDecoder",
    "BaseERC20BridgeInitiatedDecoder",
    "ETHBridgeFinalizedDecoder",
    "ERC20BridgeFinalizedDecoder",
    "LayerZeroPacketDeliveredDecoder",
    "LayerZeroPacketSentDecoder",
    "OPMessagePassedDecoder",
    "OPWithdrawalProvenDecoder",
    "OPWithdrawalFinalizedDecoder",
    "ArbitrumDepositInitiatedDecoder",
    "ArbitrumWithdrawalFinalizedDecoder",
    "ArbitrumOutBoxTransactionExecutedDecoder",
    "CowTradeDecoder",
    "ZeroExTransformedERC20Decoder",
    "OneInchOrderFilledDecoder",
    "DEFAULT_DECODERS",
]

# Bespoke decoders for events that need stateful or multi-log logic
# (bridges with cross-chain matching, aggregators that emit a generic
# `Filled` whose details are in calldata). YAML-driven decoders for the
# straightforward one-log-to-one-canonical-event cases (DEX swaps, lending,
# staking, etc.) are loaded from `core/adapters/evm/decoders/mappings/`
# at registry-build time — see `core.adapters.evm.registry`.
DEFAULT_DECODERS: list[LogDecoder] = [
    TransferDecoder(),
    ApprovalDecoder(),
    WETHDepositDecoder(),
    WETHWithdrawalDecoder(),
    StargateSendToChainDecoder(),
    StargateReceiveFromChainDecoder(),
    AcrossFundsDepositedDecoder(),
    AcrossFilledRelayDecoder(),
    BaseETHBridgeInitiatedDecoder(),
    BaseERC20BridgeInitiatedDecoder(),
    ETHBridgeFinalizedDecoder(),
    ERC20BridgeFinalizedDecoder(),
    LayerZeroPacketDeliveredDecoder(),
    LayerZeroPacketSentDecoder(),
    OPMessagePassedDecoder(),
    OPWithdrawalProvenDecoder(),
    OPWithdrawalFinalizedDecoder(),
    ArbitrumDepositInitiatedDecoder(),
    ArbitrumWithdrawalFinalizedDecoder(),
    ArbitrumOutBoxTransactionExecutedDecoder(),
    CowTradeDecoder(),
    ZeroExTransformedERC20Decoder(),
    OneInchOrderFilledDecoder(),
]
