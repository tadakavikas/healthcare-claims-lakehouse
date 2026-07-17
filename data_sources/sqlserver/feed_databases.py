"""Continuous feeder: new claims every N seconds, plus member plan-change updates.
Claims draw member_id and provider_id from the SHARED pools so gold joins match.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import time
from datetime import date, timedelta

import pymssql

from shared_ids import MEMBER_IDS, PROVIDER_IDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("feeder")

PASSWORD = os.environ["MSSQL_SA_PASSWORD"]
CLAIMS_HOST = os.environ.get("CLAIMS_HOST", "localhost")
MEMBERS_HOST = os.environ.get("MEMBERS_HOST", "localhost")

ICD10 = ["E11.9", "I10", "J45.909", "M54.5", "F41.1", "K21.9", "N39.0"]
CPT = ["99213", "99214", "80053", "36415", "71046", "93000"]
STATUS = ["SUBMITTED", "APPROVED", "DENIED", "PENDING"]
PLANS = ["PPO_GOLD", "PPO_SILVER", "HMO_STANDARD", "HDHP_HSA"]

rng = random.Random()


def feed_claims(conn) -> int:
    n = rng.randint(100, 200)
    cur = conn.cursor()
    for _ in range(n):
        cur.execute(
            """INSERT INTO ClaimsDB.dbo.medical_claims
               (claim_id, member_id, provider_id, diagnosis_code, procedure_code,
                service_date, billed_amount, allowed_amount, claim_status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (f"MC{rng.randint(10_000_000, 99_999_999)}",
             rng.choice(MEMBER_IDS), rng.choice(PROVIDER_IDS),
             rng.choice(ICD10), rng.choice(CPT),
             date.today() - timedelta(days=rng.randint(0, 30)),
             (billed := round(rng.uniform(80, 5000), 2)),
             round(billed * rng.uniform(0.4, 0.9), 2), rng.choice(STATUS)),
        )
    return n


def feed_member_updates(conn) -> int:
    cur = conn.cursor()
    updates = rng.randint(5, 10)
    cur.execute(
        f"""UPDATE m SET plan_code = %s, updated_at = SYSUTCDATETIME()
            FROM (SELECT TOP {updates} * FROM MembersDB.dbo.members ORDER BY NEWID()) m""",
        (rng.choice(PLANS),),
    )
    return updates


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval-seconds", type=int, default=600)
    parser.add_argument("--cycles", type=int, default=0)
    args = parser.parse_args()
    cycle = 0
    while True:
        cycle += 1
        try:
            with pymssql.connect(server=CLAIMS_HOST, user="sa", password=PASSWORD, autocommit=True) as c:
                n = feed_claims(c)
            with pymssql.connect(server=MEMBERS_HOST, user="sa", password=PASSWORD, autocommit=True) as c:
                upd = feed_member_updates(c)
            log.info("cycle %d: +%d claims, %d member updates", cycle, n, upd)
        except Exception:
            log.exception("cycle %d failed", cycle)
        if args.cycles and cycle >= args.cycles:
            break
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
