#!/usr/bin/env python3
"""Backfill 7 days of data across all chains using the Dagster pipeline."""
from datetime import datetime, timedelta, timezone
from dagster import materialize
from ops.dagster.nexus_pipeline.assets import raw_logs, decoded_events, bridge_links, token_prices
from ops.dagster.nexus_pipeline.resources import ClickHouseResource, EVMIngestionResource, PostgresResource
import time

WINDOW_MINUTES = 30  # Process 30 min at a time (HyperSync rate limit friendly)
TOTAL_HOURS = 168  # 7 days

# Start from the oldest data we have (April 24) and work forward
# Actually, let's just go backward from now in chunks
end = datetime.now(timezone.utc)

for i in range(TOTAL_HOURS * 60 // WINDOW_MINUTES):
    start = end - timedelta(minutes=WINDOW_MINUTES)
    window_end = end
    
    t0 = time.time()
    chunk_num = i + 1
    total_chunks = TOTAL_HOURS * 60 // WINDOW_MINUTES
    
    try:
        result = materialize(
            [raw_logs, decoded_events, bridge_links],
            resources={
                'clickhouse': ClickHouseResource(),
                'evm': EVMIngestionResource(lookback_minutes=WINDOW_MINUTES),
                'postgres': PostgresResource(),
            },
        )
        elapsed = time.time() - t0
        
        if result.success:
            raw = result.output_for_node('raw_logs')
            dec = result.output_for_node('decoded_events')
            br = result.output_for_node('bridge_links')
            print(f'[{chunk_num}/{total_chunks}] {start.strftime("%m-%d %H:%M")} → {window_end.strftime("%H:%M")} | '
                  f'raw={raw.get("raw_logs_ingested",0)} decoded={dec.get("decoded_events",0)} '
                  f'bridges_matched={br.get("matched",0)} | {elapsed:.0f}s')
        else:
            print(f'[{chunk_num}/{total_chunks}] FAILED ({elapsed:.0f}s)')
    except Exception as e:
        elapsed = time.time() - t0
        print(f'[{chunk_num}/{total_chunks}] ERROR: {e} ({elapsed:.0f}s)')
        time.sleep(5)  # Back off on error
    
    end = start  # Move window back
    
    # Rate limit courtesy — HyperSync free tier = ~3 RPM
    if chunk_num % 3 == 0:
        time.sleep(15)

print('Backfill complete!')
