"""Load pharmacy claims CSV files into Snowflake BRONZE - idempotently.

Why this version is idempotent (the interview answer):
  Snowflake's COPY load-history alone is NOT reliable for idempotency when files
  get re-uploaded (PUT OVERWRITE resets a file's load status). So instead we:

    1. PUT files into the stage (no overwrite; skip already-staged files)
    2. COPY the staged files into a TRANSIENT staging table (fresh each run)
    3. MERGE from staging into the bronze table on a business key
       (claim_id + source file). MERGE only inserts rows that aren't already
       there, so re-running the whole script never creates duplicates.

  This also defends against duplicate rows inside the source data itself
  (the CSVs intentionally contain ~2% duplicate claim_ids).

Usage:
    python3 ingestion/load_pharmacy_claims.py
"""

from __future__ import annotations

import glob
import logging
import os

import snowflake.connector

from snowflake_config import get_connection_params

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("load_pharmacy")

CSV_DIR = os.path.join(
    os.path.dirname(__file__), "..", "data", "landing", "pharmacy_claims"
)
STAGE = "@BRONZE.stg_pharmacy_claims"
BRONZE_TABLE = "BRONZE.bronze_pharmacy_claims"
STAGING_TABLE = "BRONZE.stg_load_pharmacy_claims"

COLUMNS = (
    "claim_id, member_id, ndc_code, drug_name, quantity, days_supply, "
    "fill_time, ingredient_cost, copay_amount, pharmacy_npi, _source_file"
)


def main() -> None:
    csv_files = sorted(glob.glob(os.path.join(CSV_DIR, "*.csv")))
    if not csv_files:
        log.warning("no CSV files found in %s", os.path.abspath(CSV_DIR))
        return
    log.info("found %d CSV files", len(csv_files))

    conn = snowflake.connector.connect(**get_connection_params())
    try:
        cur = conn.cursor()
        cur.execute("USE DATABASE HEALTHCARE_LAKEHOUSE")
        cur.execute("USE SCHEMA BRONZE")


        csv_dir_abs = os.path.abspath(CSV_DIR)
        cur.execute(f"PUT 'file://{csv_dir_abs}/*.csv' {STAGE} AUTO_COMPRESS=TRUE PARALLEL=8")
        log.info("staged %d files via wildcard PUT", len(csv_files))

        cur.execute(f"CREATE OR REPLACE TRANSIENT TABLE {STAGING_TABLE} LIKE {BRONZE_TABLE}")
        cur.execute(f"""
            COPY INTO {STAGING_TABLE} ({COLUMNS})
            FROM (
                SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10, METADATA$FILENAME
                FROM {STAGE}
            )
            FILE_FORMAT = (FORMAT_NAME = 'BRONZE.ff_csv')
            ON_ERROR = 'CONTINUE'
            FORCE = TRUE;
        """)
        cur.execute(f"SELECT COUNT(*) FROM {STAGING_TABLE}")
        staged_rows = cur.fetchone()[0]
        log.info("copied %d rows into staging", staged_rows)

        cur.execute(f"""
            MERGE INTO {BRONZE_TABLE} AS tgt
            USING (
                SELECT * FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY claim_id, _source_file
                            ORDER BY fill_time
                        ) AS rn
                    FROM {STAGING_TABLE}
                ) WHERE rn = 1
            ) AS src
            ON  tgt.claim_id = src.claim_id
            AND tgt._source_file = src._source_file
            WHEN NOT MATCHED THEN INSERT (
                claim_id, member_id, ndc_code, drug_name, quantity, days_supply,
                fill_time, ingredient_cost, copay_amount, pharmacy_npi, _source_file
            ) VALUES (
                src.claim_id, src.member_id, src.ndc_code, src.drug_name,
                src.quantity, src.days_supply, src.fill_time, src.ingredient_cost,
                src.copay_amount, src.pharmacy_npi, src._source_file
            );
        """)
        merge_result = cur.fetchone()
        log.info("merge result (rows inserted): %s", merge_result)

        cur.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")

        cur.execute(f"SELECT COUNT(*) FROM {BRONZE_TABLE}")
        total = cur.fetchone()[0]
        log.info("bronze_pharmacy_claims now has %d rows", total)

    finally:
        conn.close()
        log.info("connection closed")


if __name__ == "__main__":
    main()
