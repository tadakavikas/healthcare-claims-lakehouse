"""Load the two on-prem SQL Server sources into Snowflake BRONZE - incrementally.

Sources:
  ClaimsDB.dbo.medical_claims  (append-only facts)   -> bronze_medical_claims
  MembersDB.dbo.members        (rows get UPDATEd)     -> bronze_members
  MembersDB.dbo.providers      (occasional inserts)   -> bronze_providers

Pattern (the interview answer):
  For each table we keep a watermark on `updated_at`. Each run:
    1. read last watermark from BRONZE.ingestion_watermarks
    2. SELECT rows from SQL Server WHERE updated_at > watermark   (incremental)
    3. load them into a Snowflake staging table
    4. MERGE into bronze on the primary key:
         - members/providers: UPDATE existing row if the key already exists
           (this captures plan changes - true change-data-capture behaviour)
         - claims: INSERT only (append-only)
    5. advance the watermark to the max updated_at pulled

  A small lookback on the watermark tolerates clock/commit skew so no row is
  missed; the MERGE makes re-pulling harmless (idempotent).

Usage:
    python3 ingestion/load_sqlserver.py
    python3 ingestion/load_sqlserver.py --full   # ignore watermarks, pull all
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timedelta

import pymssql
import snowflake.connector

from snowflake_config import get_connection_params

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("load_sqlserver")

SA_PASSWORD = os.environ.get("MSSQL_SA_PASSWORD", "YourStrong!Passw0rd")
CLAIMS_HOST = os.environ.get("CLAIMS_HOST", "localhost")
CLAIMS_PORT = int(os.environ.get("CLAIMS_PORT", "1433"))
MEMBERS_HOST = os.environ.get("MEMBERS_HOST", "localhost")
MEMBERS_PORT = int(os.environ.get("MEMBERS_PORT", "1434"))

LOOKBACK_MINUTES = 5
DEFAULT_SINCE = "2000-01-01T00:00:00"

# Per-table config: which server, source query columns, key, and merge behaviour
TABLES = {
    "medical_claims": {
        "host": CLAIMS_HOST, "port": CLAIMS_PORT,
        "db": "ClaimsDB",
        "cols": ["claim_id", "member_id", "provider_id", "diagnosis_code",
                 "procedure_code", "service_date", "billed_amount",
                 "allowed_amount", "claim_status", "created_at", "updated_at"],
        "key": ["claim_id"],
        "bronze": "BRONZE.bronze_medical_claims",
        "update_on_match": False,   # append-only
    },
    "members": {
        "host": MEMBERS_HOST, "port": MEMBERS_PORT,
        "db": "MembersDB",
        "cols": ["member_id", "first_name", "last_name", "date_of_birth",
                 "gender", "plan_code", "state_code", "effective_date",
                 "created_at", "updated_at"],
        "key": ["member_id"],
        "bronze": "BRONZE.bronze_members",
        "update_on_match": True,    # capture plan changes
    },
    "providers": {
        "host": MEMBERS_HOST, "port": MEMBERS_PORT,
        "db": "MembersDB",
        "cols": ["provider_id", "provider_name", "npi", "specialty",
                 "state_code", "created_at", "updated_at"],
        "key": ["provider_id"],
        "bronze": "BRONZE.bronze_providers",
        "update_on_match": True,
    },
}

WATERMARK_TABLE = "BRONZE.ingestion_watermarks"


def get_watermark(cur, source: str, full: bool) -> str:
    if full:
        return DEFAULT_SINCE
    cur.execute(
        f"SELECT last_watermark FROM {WATERMARK_TABLE} WHERE source_name = %s",
        (f"sqlserver_{source}",),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return DEFAULT_SINCE
    wm = datetime.fromisoformat(row[0]) - timedelta(minutes=LOOKBACK_MINUTES)
    return wm.isoformat()


def extract_rows(cfg: dict, since: str) -> list[tuple]:
    """Pull rows changed since `since` from SQL Server."""
    conn = pymssql.connect(
        server=cfg["host"], port=cfg["port"], user="sa",
        password=SA_PASSWORD, database=cfg["db"],
    )
    try:
        cur = conn.cursor()
        col_list = ", ".join(cfg["cols"])
        cur.execute(
            f"SELECT {col_list} FROM dbo.{table_name(cfg)} "
            f"WHERE updated_at > %s ORDER BY updated_at",
            (since,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def table_name(cfg: dict) -> str:
    # reverse lookup not needed; store name on cfg at call site instead
    return cfg["_name"]


def load_table(sf_cur, name: str, cfg: dict, full: bool) -> None:
    cfg["_name"] = name
    since = get_watermark(sf_cur, name, full)
    rows = extract_rows(cfg, since)
    log.info("[%s] pulled %d rows changed since %s", name, len(rows), since)
    if not rows:
        return

    staging = f"BRONZE.stg_load_{name}"
    sf_cur.execute(f"CREATE OR REPLACE TRANSIENT TABLE {staging} LIKE {cfg['bronze']}")

    cols = cfg["cols"]
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(cols)
    sf_cur.executemany(
        f"INSERT INTO {staging} ({col_names}) VALUES ({placeholders})",
        [tuple(str(v) if v is not None else None for v in r) for r in rows],
    )

    # Build MERGE
    key_cond = " AND ".join([f"tgt.{k} = src.{k}" for k in cfg["key"]])
    dedup_partition = ", ".join(cfg["key"])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"src.{c}" for c in cols])

    update_clause = ""
    if cfg["update_on_match"]:
        set_cols = [c for c in cols if c not in cfg["key"]]
        set_expr = ", ".join([f"tgt.{c} = src.{c}" for c in set_cols])
        update_clause = f"WHEN MATCHED THEN UPDATE SET {set_expr}"

    sf_cur.execute(f"""
        MERGE INTO {cfg['bronze']} AS tgt
        USING (
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY {dedup_partition} ORDER BY updated_at DESC
                ) AS rn FROM {staging}
            ) WHERE rn = 1
        ) AS src
        ON {key_cond}
        {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
    """)
    log.info("[%s] merge result: %s", name, sf_cur.fetchone())

    new_wm = max(str(r[cols.index("updated_at")]) for r in rows)
    sf_cur.execute(f"""
        MERGE INTO {WATERMARK_TABLE} AS t
        USING (SELECT %s AS source_name, %s AS last_watermark) AS s
        ON t.source_name = s.source_name
        WHEN MATCHED THEN UPDATE SET last_watermark = s.last_watermark,
                                     updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (source_name, last_watermark)
                             VALUES (s.source_name, s.last_watermark)
    """, (f"sqlserver_{name}", new_wm))
    sf_cur.execute(f"DROP TABLE IF EXISTS {staging}")
    log.info("[%s] watermark advanced to %s", name, new_wm)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    conn = snowflake.connector.connect(**get_connection_params())
    try:
        cur = conn.cursor()
        cur.execute("USE DATABASE HEALTHCARE_LAKEHOUSE")
        cur.execute("USE SCHEMA BRONZE")
        for name, cfg in TABLES.items():
            load_table(cur, name, cfg, args.full)
        for name, cfg in TABLES.items():
            cur.execute(f"SELECT COUNT(*) FROM {cfg['bronze']}")
            log.info("%s total rows: %d", cfg["bronze"], cur.fetchone()[0])
    finally:
        conn.close()
        log.info("connection closed")


if __name__ == "__main__":
    main()
