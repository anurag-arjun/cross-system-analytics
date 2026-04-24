"""ERC-20 and token transfer decoders."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from core.adapters.evm.decoders.base import DecodedEvent, LogDecoder


class TransferDecoder(LogDecoder):
    """ERC-20 Transfer: Transfer(address indexed from, address indexed to, uint256 value)"""

    @property
    def topic0(self) -> str:
        return "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    @property
    def event_signature(self) -> str:
        return "Transfer(address indexed from, address indexed to, uint256 value)"

    @property
    def protocol(self) -> str:
        return "erc20"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 3:
            return None
        sender = self._topic_address(topics[1])
        receiver = self._topic_address(topics[2])
        data = log.get("data", "0x0")
        amount = Decimal(int(data, 16)) if data not in ("0x", "") else Decimal(0)

        return DecodedEvent(
            event_type="transfer_out",
            entity_id=sender,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            token_out=log["address"],
            amount_out=amount,
            counterparty=receiver,
            extra={"receiver": receiver},
        )


class ApprovalDecoder(LogDecoder):
    """ERC-20 Approval: Approval(address indexed owner, address indexed spender, uint256 value)"""

    @property
    def topic0(self) -> str:
        return "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

    @property
    def event_signature(self) -> str:
        return "Approval(address,address,uint256)"

    @property
    def protocol(self) -> str:
        return "erc20"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 3:
            return None
        owner = self._topic_address(topics[1])
        spender = self._topic_address(topics[2])
        data = log.get("data", "0x0")
        amount = Decimal(int(data, 16)) if data not in ("0x", "") else Decimal(0)

        return DecodedEvent(
            event_type="approval",
            entity_id=owner,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            token_out=log["address"],
            amount_out=amount,
            counterparty=spender,
            extra={"spender": spender},
        )


class WETHDepositDecoder(LogDecoder):
    """WETH Deposit: Deposit(address,uint256)"""

    @property
    def topic0(self) -> str:
        return "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c"

    @property
    def event_signature(self) -> str:
        return "Deposit(address,uint256)"

    @property
    def protocol(self) -> str:
        return "weth"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 2:
            return None
        dst = self._topic_address(topics[1])
        data = log.get("data", "0x0")
        amount = Decimal(int(data, 16)) if data not in ("0x", "") else Decimal(0)

        return DecodedEvent(
            event_type="deposit",
            entity_id=dst,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            token_out="ETH",
            amount_out=amount,
            extra={"action": "wrap_eth"},
        )


class WETHWithdrawalDecoder(LogDecoder):
    """WETH Withdrawal: Withdrawal(address,uint256)"""

    @property
    def topic0(self) -> str:
        return "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65"

    @property
    def event_signature(self) -> str:
        return "Withdrawal(address,uint256)"

    @property
    def protocol(self) -> str:
        return "weth"

    def decode(self, log: dict[str, Any], timestamp: datetime) -> DecodedEvent | None:
        topics = log.get("topics", [])
        if len(topics) < 2:
            return None
        src = self._topic_address(topics[1])
        data = log.get("data", "0x0")
        amount = Decimal(int(data, 16)) if data not in ("0x", "") else Decimal(0)

        return DecodedEvent(
            event_type="withdrawal",
            entity_id=src,
            timestamp=timestamp,
            block_number=int(log["blockNumber"], 16),
            tx_hash=log["transactionHash"],
            log_index=int(log["logIndex"], 16),
            protocol=self.protocol,
            venue=log["address"],
            token_out=log["address"],
            amount_out=amount,
            extra={"action": "unwrap_eth"},
        )
