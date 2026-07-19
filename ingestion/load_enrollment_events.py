"""Load enrollment events from the REST API into Snowflake BRONZE - incrementally.

Why this is different from the CSV loader (the interview answer):
  The API is a live endpoint, not files. So the pattern is:
    1. Read the last watermark (max event_time already loaded) from a control table
    2. Call the API with ?since=<watermark>, paging through with the cursor
    3. Load the pulled events into a staging table
    4. MERGE into bronze on event_id (idempotent - re-runs never duplicate)
    5. Advance the watermark to the newest event_time we saw

  Watermark + lookback:
    We subtract a LOOKBACK_HOURS buffer from the watermark before calling the API.
    This re-pulls a small recent window every run so late-arriving events (whose
    event_time is older than when they were recorded) are not missed. The MERGE
    makes re-pulling harmless. This is the standard fix for late data.

Usage:
    python3 ingestion/load_enrollment_events.py
    python3 ingestion/load_enrollment_events.py --full   # ignore watermark, pull all
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timedelta

import requests
import snowflake.connector

from snowflake_config import get_connection_params

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("load_enrollment")

API_URL = os.environ.get("ENROLLMENT_API_URL", "http://localhost:8000/api/v1/enrollment-events")
SOURCE_NAME = "enrollment_api"
BRONZE_TABLE = "BRONZE.bronze_enrollment_events"
STAGING_TABLE = "BRONZE.stg_load_enrollment_events"
WATERMARK_TABLE = "BRONZE.ingestion_watermarks"

LOOKBACK_HOURS = 6        # re-pull this window to catch late-arriving events
DEFAULT_SINCE = "2000-01-01T00:00:00"
PAGE_SIZE = 500


def get_watermark(cur, full: bool) -> str:
    """Read last watermark; on --full or first run, start from the beginning."""
    if full:
        return DEFAULT_SINCE
    cur.execute(
        f"SELECT last_watermark FROM {WATERMARK_TABLE} WHERE source_name = %s",
        (SOURCE_NAME,),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return DEFAULT_SINCE
    # apply lookback so late events in the recent window get re-pulled
    wm = datetime.fromisoformat(row[0]) - timedelta(hours=LOOKBACK_HOURS)
    return wm.isoformat()


def fetch_events(since: str) -> list[dict]:
    """Page through the API from `since`, following the cursor to the end."""
    events: list[dict] = []
    cursor = 0
    while True:
        resp = requests.get(
            API_URL,
            params={"since": since, "limit": PAGE_SIZE, "cursor": cursor},
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        events.extend(payload["data"])
        nxt = payload.get("next_cursor")
        if nxt is None:
            break
        cursor = nxt
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full", action="store_true", help="ignore watermark, pull everything")
    args = parser.parse_args()

    conn = snowflake.connector.connect(**get_connection_params())
    try:
        cur = conn.cursor()
        cur.execute("USE DATABASE HEALTHCARE_LAKEHOUSE")
        cur.execute("USE SCHEMA BRONZE")

        since = get_watermark(cur, args.full)
        log.info("pulling events since %s (lookback applied)", since)

        events = fetch_events(since)
        log.info("fetched %d events from API", len(events))
        if not events:
            log.info("nothing new to load")
            return

        # Fresh staging table, insert the pulled rows
        cur.execute(f"CREATE OR REPLACE TRANSIENT TABLE {STAGING_TABLE} LIKE {BRONZE_TABLE}")
        rows = [
            (e["event_id"], e["member_id"], e["event_type"],
             e["plan_code"], e["event_time"], e["created_at"])
            for e in events
        ]
        cur.executemany(
            f"""INSERT INTO {STAGING_TABLE}
                (event_id, member_id, event_type, plan_code, event_time, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)""",
            rows,
        )
        log.info("inserted %d rows into staging", len(rows))

        # MERGE into bronze on event_id -> idempotent
        cur.execute(f"""
            MERGE INTO {BRONZE_TABLE} AS tgt
            USING (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY event_id ORDER BY created_at DESC
                    ) AS rn
                    FROM {STAGING_TABLE}
                ) WHERE rn = 1
            ) AS src
            ON tgt.event_id = src.event_id
            WHEN NOT MATCHED THEN INSERT
                (event_id, member_id, event_type, plan_code, event_time, created_at)
                VALUES (src.event_id, src.member_id, src.event_type,
                        src.plan_code, src.event_time, src.created_at);
        """)
        log.info("merge inserted: %s", cur.fetchone())

        # Advance the watermark to the newest event_time we loaded
        new_wm = max(e["event_time"] for e in events)
        cur.execute(
            f"""MERGE INTO {WATERMARK_TABLE} AS t
                USING (SELECT %s AS source_name, %s AS last_watermark) AS s
                ON t.source_name = s.source_name
                WHEN MATCHED THEN UPDATE SET last_watermark = s.last_watermark,
                                             updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (source_name, last_watermark)
                                     VALUES (s.source_name, s.last_watermark)""",
            (SOURCE_NAME, new_wm),
        )
        log.info("watermark advanced to %s", new_wm)

        cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
        cur.execute(f"SELECT COUNT(*) FROM {BRONZE_TABLE}")
        log.info("bronze_enrollment_events now has %d rows", cur.fetchone()[0])

    finally:
        conn.close()
        log.info("connection closed")


if __name__ == "__main__":
    main()
