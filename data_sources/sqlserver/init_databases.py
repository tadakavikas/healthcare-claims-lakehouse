"""One-time initializer: creates databases and tables on both SQL Server instances.

Source 3 (sqlserver-claims):  ClaimsDB.dbo.medical_claims   - append-only OLTP facts
Source 4 (sqlserver-members): MembersDB.dbo.members         - master data, rows get UPDATEd
                              MembersDB.dbo.providers
"""

from __future__ import annotations

import logging
import os
import time

import pymssql

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("init")

PASSWORD = os.environ["MSSQL_SA_PASSWORD"]
CLAIMS_HOST = os.environ.get("CLAIMS_HOST", "localhost")
MEMBERS_HOST = os.environ.get("MEMBERS_HOST", "localhost")

CLAIMS_DDL = """
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'ClaimsDB')
    CREATE DATABASE ClaimsDB;
"""

CLAIMS_TABLE = """
IF NOT EXISTS (SELECT 1 FROM ClaimsDB.sys.tables WHERE name = 'medical_claims')
CREATE TABLE ClaimsDB.dbo.medical_claims (
    claim_id        VARCHAR(20)  NOT NULL PRIMARY KEY,
    member_id       VARCHAR(10)  NOT NULL,
    provider_id     VARCHAR(10)  NOT NULL,
    diagnosis_code  VARCHAR(10)  NOT NULL,   -- ICD-10
    procedure_code  VARCHAR(10)  NOT NULL,   -- CPT
    service_date    DATE         NOT NULL,
    billed_amount   DECIMAL(10,2) NOT NULL,
    allowed_amount  DECIMAL(10,2) NOT NULL,
    claim_status    VARCHAR(12)  NOT NULL,
    created_at      DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME()
);
"""

MEMBERS_DDL = """
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'MembersDB')
    CREATE DATABASE MembersDB;
"""

MEMBERS_TABLES = [
    """
IF NOT EXISTS (SELECT 1 FROM MembersDB.sys.tables WHERE name = 'members')
CREATE TABLE MembersDB.dbo.members (
    member_id     VARCHAR(10) NOT NULL PRIMARY KEY,
    first_name    VARCHAR(50) NOT NULL,
    last_name     VARCHAR(50) NOT NULL,
    date_of_birth DATE        NOT NULL,
    gender        CHAR(1)     NOT NULL,
    plan_code     VARCHAR(20) NOT NULL,
    state_code    CHAR(2)     NOT NULL,
    effective_date DATE       NOT NULL,
    created_at    DATETIME2   NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at    DATETIME2   NOT NULL DEFAULT SYSUTCDATETIME()
);
""",
    """
IF NOT EXISTS (SELECT 1 FROM MembersDB.sys.tables WHERE name = 'providers')
CREATE TABLE MembersDB.dbo.providers (
    provider_id   VARCHAR(10) NOT NULL PRIMARY KEY,
    provider_name VARCHAR(100) NOT NULL,
    npi           VARCHAR(10)  NOT NULL,
    specialty     VARCHAR(50)  NOT NULL,
    state_code    CHAR(2)      NOT NULL,
    created_at    DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at    DATETIME2    NOT NULL DEFAULT SYSUTCDATETIME()
);
""",
]


def connect_with_retry(host: str, attempts: int = 12) -> pymssql.Connection:
    for i in range(attempts):
        try:
            return pymssql.connect(server=host, user="sa", password=PASSWORD, autocommit=True)
        except Exception as exc:  # noqa: BLE001 - retry on any startup failure
            log.warning("waiting for %s (%d/%d): %s", host, i + 1, attempts, exc)
            time.sleep(10)
    raise RuntimeError(f"could not connect to {host}")


def main() -> None:
    with connect_with_retry(CLAIMS_HOST) as conn:
        cur = conn.cursor()
        cur.execute(CLAIMS_DDL)
        cur.execute(CLAIMS_TABLE)
        log.info("ClaimsDB ready on %s", CLAIMS_HOST)

    with connect_with_retry(MEMBERS_HOST) as conn:
        cur = conn.cursor()
        cur.execute(MEMBERS_DDL)
        for ddl in MEMBERS_TABLES:
            cur.execute(ddl)
        log.info("MembersDB ready on %s", MEMBERS_HOST)


if __name__ == "__main__":
    main()
