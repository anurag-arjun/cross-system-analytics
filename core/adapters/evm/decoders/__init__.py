from core.adapters.evm.decoders.base import DecodedEvent, LogDecoder
from core.adapters.evm.decoders.bridge import (
    AcrossV3FilledRelayDecoder,
    AcrossV3FundsDepositedDecoder,
    BaseERC20BridgeInitiatedDecoder,
    BaseETHBridgeInitiatedDecoder,
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
    "AcrossV3FundsDepositedDecoder",
    "AcrossV3FilledRelayDecoder",
    "BaseETHBridgeInitiatedDecoder",
    "BaseERC20BridgeInitiatedDecoder",
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
    AcrossV3FundsDepositedDecoder(),
    AcrossV3FilledRelayDecoder(),
    BaseETHBridgeInitiatedDecoder(),
    BaseERC20BridgeInitiatedDecoder(),
]
