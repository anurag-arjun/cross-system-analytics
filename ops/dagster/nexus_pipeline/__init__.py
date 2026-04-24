from dagster import Definitions, load_assets_from_modules

from . import assets
from .resources import ClickHouseResource, EVMIngestionResource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "clickhouse": ClickHouseResource(),
        "evm": EVMIngestionResource(),
    },
)
