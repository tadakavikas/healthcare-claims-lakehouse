"""Continuous feeder: writes new data to both SQL Server sources every N seconds.

Every cycle (default 600s = 10 min):
  ClaimsDB.dbo.medical_claims  -> 100-200 new INSERTs (append-only fact stream)
  MembersDB.dbo.members        -> 10-20 new members + 5-10 UPDATEs to existing rows
  MembersDB.dbo.providers      -> occasional new provider

The UPDATEs on members matter: they force the ingestion layer to do
incremental extraction on `updated_at`, not just on the primary key.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import time
from datetime import date, timedelta

import pymssql

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("feeder")

PASSWORD = os.environ["MSSQL_SA_PASSWORD"]
CLAIMS_HOST = os.environ.get("CLAIMS_HOST", "localhost")
MEMBERS_HOST = os.environ.get("MEMBERS_HOST", "localhost")

ICD10 = ["E11.9", "I10", "J45.909", "M54.5", "F41.1", "K21.9", "N39.0"]
CPT = ["99213", "99214", "80053", "36415", "71046", "93000"]
STATUS = ["SUBMITTED", "APPROVED", "DENIED", "PENDING"]
FIRST = ["James", "Maria", "Wei", "Aisha", "Carlos", "Priya", "John", "Fatima"]
LAST = ["Smith", "Garcia", "Chen", "Patel", "Johnson", "Nguyen", "Brown", "Khan"]
SPECIALTIES = ["Internal Medicine", "Cardiology", "Orthopedics", "Family Medicine", "Endocrinology"]
STATES = ["NC", "GA", "SC", "VA", "TN"]
PLANS = ["PPO_GOLD", "PPO_SILVER", "HMO_STANDARD", "HDHP_HSA"]

rng = random.Random()


def feed_claims(conn: pymssql.Connection) -> int:
    n = rng.randint(100, 200)
    cur = conn.cursor()
    for _ in range(n):
        cur.execute(
            """INSERT INTO ClaimsDB.dbo.medical_claims
               (claim_id, member_id, provider_id, diagnosis_code, procedure_code,
                service_date, billed_amount, allowed_amount, claim_status)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                f"MC{rng.randint(10_000_000, 99_999_999)}",
                f"M{rng.randint(100_000, 199_999)}",
                f"P{rng.randint(1000, 9999)}",
                rng.choice(ICD10),
                rng.choice(CPT),
                date.today() - timedelta(days=rng.randint(0, 30)),
                (billed := round(rng.uniform(80, 5000), 2)),
                round(billed * rng.uniform(0.4, 0.9), 2),
                rng.choice(STATUS),
            ),
        )
    return n


def feed_members(conn: pymssql.Connection) -> tuple[int, int]:
    cur = conn.cursor()
    inserts = rng.randint(10, 20)
    for _ in range(inserts):
        cur.execute(
            """INSERT INTO MembersDB.dbo.members
               (member_id, first_name, last_name, date_of_birth, gender,
                plan_code, state_code, effective_date)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                f"M{rng.randint(100_000, 199_999)}",
                rng.choice(FIRST),
                rng.choice(LAST),
                date(rng.randint(1950, 2005), rng.randint(1, 12), rng.randint(1, 28)),
                rng.choice(["M", "F"]),
                rng.choice(PLANS),
                rng.choice(STATES),
                date.today(),
            ),
        )
    # Simulate plan changes: update a few existing members (bumps updated_at)
    updates = rng.randint(5, 10)
    cur.execute(
        f"""UPDATE m SET plan_code = %s, updated_at = SYSUTCDATETIME()
            FROM (SELECT TOP {updates} * FROM MembersDB.dbo.members
                  ORDER BY NEWID()) m""",
        (rng.choice(PLANS),),
    )
    if rng.random() < 0.3:
        cur.execute(
            """INSERT INTO MembersDB.dbo.providers
               (provider_id, provider_name, npi, specialty, state_code)
               VALUES (%s, %s, %s, %s, %s)""",
            (
                f"P{rng.randint(1000, 9999)}",
                f"Dr. {rng.choice(FIRST)} {rng.choice(LAST)}",
                str(rng.randint(1_000_000_000, 1_999_999_999)),
                rng.choice(SPECIALTIES),
                rng.choice(STATES),
            ),
        )
    return inserts, updates


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval-seconds", type=int, default=600)
    parser.add_argument("--cycles", type=int, default=0, help="0 = run forever")
    args = parser.parse_args()

    cycle = 0
    while True:
        cycle += 1
        try:
            with pymssql.connect(server=CLAIMS_HOST, user="sa", password=PASSWORD, autocommit=True) as c:
                n = feed_claims(c)
            with pymssql.connect(server=MEMBERS_HOST, user="sa", password=PASSWORD, autocommit=True) as c:
                ins, upd = feed_members(c)
            log.info("cycle %d: +%d claims, +%d members, %d member updates", cycle, n, ins, upd)
        except Exception:
            log.exception("cycle %d failed, retrying next interval", cycle)

        if args.cycles and cycle >= args.cycles:
            break
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
