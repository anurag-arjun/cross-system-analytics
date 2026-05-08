"""Protocol contract registry — maps (chain, address) -> protocol.

Backs the address-first lookup path in the EVM decoder registry. Populated
by the spellbook seed importer (this module) and the Dune bootstrap script
(separate ticket).
"""

from core.registry.abi_fetcher import ABIFetcher, FetchStats, parse_abi
from core.registry.abi_store import (
    ABIRecord,
    ABIStore,
    BytecodeRecord,
    BytecodeStore,
    InMemoryABIStore,
    InMemoryBytecodeStore,
    PostgresABIStore,
    PostgresBytecodeStore,
)
from core.registry.contract_labels import (
    ContractLabel,
    ContractLabelStore,
    InMemoryContractLabelStore,
    PostgresContractLabelStore,
)
from core.registry.protocol_contracts import (
    InMemoryProtocolContractStore,
    PostgresProtocolContractStore,
    ProtocolContract,
    ProtocolContractStore,
    make_resolver,
)

__all__ = [
    "ProtocolContract",
    "ProtocolContractStore",
    "InMemoryProtocolContractStore",
    "PostgresProtocolContractStore",
    "make_resolver",
    "ContractLabel",
    "ContractLabelStore",
    "InMemoryContractLabelStore",
    "PostgresContractLabelStore",
    "ABIRecord",
    "ABIStore",
    "InMemoryABIStore",
    "PostgresABIStore",
    "BytecodeRecord",
    "BytecodeStore",
    "InMemoryBytecodeStore",
    "PostgresBytecodeStore",
    "ABIFetcher",
    "FetchStats",
    "parse_abi",
]
