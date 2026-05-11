# Continuous ingestion via cron

The Dagster `dagster dev` server was being used during development as both the
asset-DAG documentation tool AND the schedule daemon. In practice the
schedule never reliably fired (no long-running daemon), so this project
runs ingestion via plain cron + the `ops/run_ingestion.py` wrapper.

`run_ingestion.py` materialises the Dagster `raw_logs`, `decoded_events`,
`bridge_links`, `token_prices`, and (Mon 06:00 UTC) `dune_parity` assets
in a single subprocess and exits — no daemon, no UI.

## Install on your laptop / VPS

Edit your crontab (`crontab -e`) and add:

```cron
# Nexus Analytics — hourly ingestion (each pass covers the last 60 minutes)
0 * * * * cd /path/to/nexus-analytics && PYTHONPATH=. /path/to/python ops/run_ingestion.py >> /var/log/nexus_ingest.log 2>&1
```

Find your Python with `which python` or `which python3`. The script uses
`load_dotenv` so the project's `.env` must be present in the repo root.

### On the VPS (apnetv@shieldtx-vps)

```bash
# After git pull
crontab -e
# Paste the entry above with the actual repo path.
```

Log rotation: add `/etc/logrotate.d/nexus_ingest` so the log doesn't grow
forever — example weekly rotation:

```
/var/log/nexus_ingest.log {
    weekly
    rotate 4
    compress
    missingok
    notifempty
}
```

## Verify it's working

```bash
tail -20 /var/log/nexus_ingest.log
# Expect every hour:
#   INFO ops.run_ingestion starting materialize: assets=... lookback=60m
#   INFO ... Fetched N raw logs from <chain>
#   INFO ops.run_ingestion DONE in <seconds>s
```

If you see `FAILED` lines, run manually with `--verbose` to debug.

## One-shot backfill

For historical fill — e.g. when adding a chain or extending the BD-doc
window from 7 to 30 days — use `ops/run_backfill.py` instead:

```bash
# Last 7 days, all chains
PYTHONPATH=. python ops/run_backfill.py --days 7

# Specific window, specific chain
PYTHONPATH=. python ops/run_backfill.py \
    --start 2026-04-11T00:00:00 --end 2026-05-11T00:00:00 \
    --chains polygon

# Tail what it's doing
PYTHONPATH=. python ops/run_backfill.py --days 7 > /tmp/backfill.log 2>&1 &
tail -f /tmp/backfill.log
```

The script is idempotent — the ClickHouse sinks dedupe on `event_id` and
`(tx_hash, log_index)`, so re-running an overlapping window doesn't
double-count.
