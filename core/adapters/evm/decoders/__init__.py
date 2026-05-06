from core.adapters.evm.decoders.aggregator import (
    CowTradeDecoder,
    OneInchOrderFilledDecoder,
    ZeroExTransformedERC20Decoder,
)
from core.adapters.evm.decoders.base import DecodedEvent, LogDecoder
from core.adapters.evm.decoders.bridge import (
    AcrossFilledRelayDecoder,
    AcrossFundsDepositedDecoder,
    BaseERC20BridgeInitiatedDecoder,
    BaseETHBridgeInitiatedDecoder,
    ERC20BridgeFinalizedDecoder,
    ETHBridgeFinalizedDecoder,
    LayerZeroPacketDeliveredDecoder,
    StargateReceiveFromChainDecoder,
    StargateSendToChainDecoder,
)

from core.adapters.evm.decoders.dex import UniswapV2SwapDecoder, UniswapV3SwapDecoder
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
    "UniswapV2SwapDecoder",
    "UniswapV3SwapDecoder",
    "StargateSendToChainDecoder",
    "StargateReceiveFromChainDecoder",
    "AcrossFundsDepositedDecoder",
    "AcrossFilledRelayDecoder",
    "BaseETHBridgeInitiatedDecoder",
    "BaseERC20BridgeInitiatedDecoder",
    "ETHBridgeFinalizedDecoder",
    "ERC20BridgeFinalizedDecoder",
    "LayerZeroPacketDeliveredDecoder",
    "CowTradeDecoder",
    "ZeroExTransformedERC20Decoder",
    "OneInchOrderFilledDecoder",
    "DEFAULT_DECODERS",
]

DEFAULT_DECODERS: list[LogDecoder] = [
    TransferDecoder(),
    ApprovalDecoder(),
    WETHDepositDecoder(),
    WETHWithdrawalDecoder(),
    UniswapV2SwapDecoder(),
    UniswapV3SwapDecoder(),
    StargateSendToChainDecoder(),
    StargateReceiveFromChainDecoder(),
    AcrossFundsDepositedDecoder(),
    AcrossFilledRelayDecoder(),
    BaseETHBridgeInitiatedDecoder(),
    BaseERC20BridgeInitiatedDecoder(),
    ETHBridgeFinalizedDecoder(),
    ERC20BridgeFinalizedDecoder(),
    LayerZeroPacketDeliveredDecoder(),
    CowTradeDecoder(),
    ZeroExTransformedERC20Decoder(),
    OneInchOrderFilledDecoder(),
]
