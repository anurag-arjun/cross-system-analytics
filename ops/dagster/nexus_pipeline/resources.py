"""Dagster resources for Nexus Analytics pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import clickhouse_connect
from dagster import ConfigurableResource

from core.adapters.evm.multi import ChainConfig, MultiChainAdapter
from core.identity.bridge_links import BridgeLinkEngine
from core.sink import BridgeLinkSink, ClickHouseSink, RawLogSink, SinkConfig


@dataclass
class IngestionConfig:
    chains: list[ChainConfig]
    lookback_minutes: int = 30
    addresses: list[str] | None = None
    hyper_token: str | None = None


class ClickHouseResource(ConfigurableResource):
    host: str = "localhost"
    port: int = 8124
    username: str = "default"
    password: str = "nexus"
    database: str = "default"

    def get_client(self):
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )

    def get_event_sink(self, batch_size: int = 5000) -> ClickHouseSink:
        return ClickHouseSink(
            SinkConfig(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                table="canonical_events",
                batch_size=batch_size,
            )
        )

    def get_raw_log_sink(self, batch_size: int = 5000) -> RawLogSink:
        return RawLogSink(
            SinkConfig(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                table="canonical_logs",
                batch_size=batch_size,
            )
        )

    def get_bridge_link_sink(self, batch_size: int = 5000) -> BridgeLinkSink:
        return BridgeLinkSink(
            SinkConfig(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                table="bridge_links",
                batch_size=batch_size,
            )
        )


class EVMIngestionResource(ConfigurableResource):
    lookback_minutes: int = 30
    hyper_token: str | None = None

    def get_adapter(self, chains: list[ChainConfig]) -> MultiChainAdapter:
        return MultiChainAdapter(
            chains=chains,
            hyper_token=self.hyper_token or os.getenv("HYPERSYNC_TOKEN"),
        )

    def get_bridge_engine(self) -> BridgeLinkEngine:
        return BridgeLinkEngine()
