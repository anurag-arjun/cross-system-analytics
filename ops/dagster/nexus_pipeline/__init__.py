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

dune_parity_job = define_asset_job(
    name="dune_parity_job",
    selection=["dune_parity"],
)

# Mondays 06:00 UTC. The check is one Dune query per run (~5 credits) so
# weekly is comfortable on the free tier (~22 credits/month).
weekly_dune_parity = ScheduleDefinition(
    name="weekly_dune_parity",
    cron_schedule="0 6 * * 1",
    job=dune_parity_job,
)

defs = Definitions(
    assets=all_assets,
    jobs=[ingestion_job, dune_parity_job],
    schedules=[hourly_ingestion, weekly_dune_parity],
    resources={
        "clickhouse": ClickHouseResource(),
        "evm": EVMIngestionResource(lookback_minutes=60),
        "postgres": PostgresResource(),
    },
)
