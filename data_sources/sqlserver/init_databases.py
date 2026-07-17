"""One-time initializer: creates databases/tables AND seeds the full member and
provider pools, so claims (which draw from the same pools) always match.
"""

from __future__ import annotations

import logging
import os
import random
import time
from datetime import date

import pymssql

from shared_ids import MEMBER_IDS, PROVIDER_IDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("init")

PASSWORD = os.environ["MSSQL_SA_PASSWORD"]
CLAIMS_HOST = os.environ.get("CLAIMS_HOST", "localhost")
MEMBERS_HOST = os.environ.get("MEMBERS_HOST", "localhost")

FIRST = ["James", "Maria", "Wei", "Aisha", "Carlos", "Priya", "John", "Fatima"]
LAST = ["Smith", "Garcia", "Chen", "Patel", "Johnson", "Nguyen", "Brown", "Khan"]
SPECIALTIES = ["Internal Medicine", "Cardiology", "Orthopedics", "Family Medicine", "Endocrinology"]
STATES = ["NC", "GA", "SC", "VA", "TN"]
PLANS = ["PPO_GOLD", "PPO_SILVER", "HMO_STANDARD", "HDHP_HSA"]

rng = random.Random(7)

CLAIMS_TABLE = """
IF NOT EXISTS (SELECT 1 FROM ClaimsDB.sys.tables WHERE name = 'medical_claims')
CREATE TABLE ClaimsDB.dbo.medical_claims (
    claim_id VARCHAR(20) NOT NULL PRIMARY KEY, member_id VARCHAR(10) NOT NULL,
    provider_id VARCHAR(10) NOT NULL, diagnosis_code VARCHAR(10) NOT NULL,
    procedure_code VARCHAR(10) NOT NULL, service_date DATE NOT NULL,
    billed_amount DECIMAL(10,2) NOT NULL, allowed_amount DECIMAL(10,2) NOT NULL,
    claim_status VARCHAR(12) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
"""

MEMBERS_TABLE = """
IF NOT EXISTS (SELECT 1 FROM MembersDB.sys.tables WHERE name = 'members')
CREATE TABLE MembersDB.dbo.members (
    member_id VARCHAR(10) NOT NULL PRIMARY KEY, first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL, date_of_birth DATE NOT NULL, gender CHAR(1) NOT NULL,
    plan_code VARCHAR(20) NOT NULL, state_code CHAR(2) NOT NULL, effective_date DATE NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
"""

PROVIDERS_TABLE = """
IF NOT EXISTS (SELECT 1 FROM MembersDB.sys.tables WHERE name = 'providers')
CREATE TABLE MembersDB.dbo.providers (
    provider_id VARCHAR(10) NOT NULL PRIMARY KEY, provider_name VARCHAR(100) NOT NULL,
    npi VARCHAR(10) NOT NULL, specialty VARCHAR(50) NOT NULL, state_code CHAR(2) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME());
"""


def connect(host, db=None, attempts=12):
    for i in range(attempts):
        try:
            return pymssql.connect(server=host, user="sa", password=PASSWORD, database=db, autocommit=True)
        except Exception as exc:
            log.warning("waiting for %s (%d/%d): %s", host, i + 1, attempts, exc)
            time.sleep(10)
    raise RuntimeError(f"could not connect to {host}")


def main() -> None:
    with connect(CLAIMS_HOST) as c:
        cur = c.cursor()
        cur.execute("IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name='ClaimsDB') CREATE DATABASE ClaimsDB;")
        cur.execute(CLAIMS_TABLE)
        log.info("ClaimsDB ready")

    with connect(MEMBERS_HOST) as c:
        cur = c.cursor()
        cur.execute("IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name='MembersDB') CREATE DATABASE MembersDB;")
        cur.execute(MEMBERS_TABLE)
        cur.execute(PROVIDERS_TABLE)
        log.info("MembersDB ready")

        # Seed the FULL member pool
        cur.execute("SELECT COUNT(*) FROM MembersDB.dbo.members")
        if cur.fetchone()[0] == 0:
            for mid in MEMBER_IDS:
                cur.execute(
                    """INSERT INTO MembersDB.dbo.members
                       (member_id, first_name, last_name, date_of_birth, gender,
                        plan_code, state_code, effective_date)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (mid, rng.choice(FIRST), rng.choice(LAST),
                     date(rng.randint(1950, 2005), rng.randint(1, 12), rng.randint(1, 28)),
                     rng.choice(["M", "F"]), rng.choice(PLANS), rng.choice(STATES), date.today()),
                )
            log.info("seeded %d members", len(MEMBER_IDS))

        # Seed the FULL provider pool
        cur.execute("SELECT COUNT(*) FROM MembersDB.dbo.providers")
        if cur.fetchone()[0] == 0:
            for pid in PROVIDER_IDS:
                cur.execute(
                    """INSERT INTO MembersDB.dbo.providers
                       (provider_id, provider_name, npi, specialty, state_code)
                       VALUES (%s,%s,%s,%s,%s)""",
                    (pid, f"Dr. {rng.choice(FIRST)} {rng.choice(LAST)}",
                     str(rng.randint(1_000_000_000, 1_999_999_999)),
                     rng.choice(SPECIALTIES), rng.choice(STATES)),
                )
            log.info("seeded %d providers", len(PROVIDER_IDS))


if __name__ == "__main__":
    main()
