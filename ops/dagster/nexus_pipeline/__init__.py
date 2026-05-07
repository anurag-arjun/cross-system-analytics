from dagster import Definitions, ScheduleDefinition, define_asset_job, load_assets_from_modules

from . import assets
from .resources import ClickHouseResource, EVMIngestionResource, PostgresResource

all_assets = load_assets_from_modules([assets])

ingestion_job = define_asset_job(
    name="ingestion_job",
    selection=["raw_logs", "decoded_events", "bridge_links"],
)

hourly_ingestion = ScheduleDefinition(
    name="hourly_ingestion",
    cron_schedule="0 * * * *",
    job=ingestion_job,
)

defs = Definitions(
    assets=all_assets,
    jobs=[ingestion_job],
    schedules=[hourly_ingestion],
    resources={
        "clickhouse": ClickHouseResource(),
        "evm": EVMIngestionResource(lookback_minutes=60),
        "postgres": PostgresResource(),
    },
)
