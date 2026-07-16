"""Central Snowflake connection config, read from environment variables (.env).

Keeping credentials in .env (never in code) is the reason this file only reads
os.environ and holds no secrets itself. The .env file is git-ignored.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()  # reads the .env file in the project root into environment variables


def get_connection_params() -> dict:
    """Build the dict that snowflake.connector.connect() expects."""
    required = {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing Snowflake credentials in .env: {', '.join(missing)}. "
            "Copy .env.example to .env and fill them in."
        )

    return {
        **required,
        "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "LAKEHOUSE_WH"),
        "database": os.environ.get("SNOWFLAKE_DATABASE", "HEALTHCARE_LAKEHOUSE"),
    }
