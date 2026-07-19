"""Hourly end-to-end pipeline: ingest four sources, then transform with dbt."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/project"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}

with DAG(
    dag_id="healthcare_lakehouse_hourly",
    description="Ingest four sources into bronze, then build silver and gold with dbt",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "medallion", "snowflake"],
) as dag:

    generate_csv = BashOperator(
        task_id="generate_hourly_csv",
        bash_command=f"cd {PROJECT} && python data_sources/csv_generator/generate_hourly_csv.py --hours 1",
    )

    load_pharmacy = BashOperator(
        task_id="load_pharmacy_claims",
        bash_command=f"cd {PROJECT} && python ingestion/load_pharmacy_claims.py",
    )

    load_enrollment = BashOperator(
        task_id="load_enrollment_events",
        bash_command=f"cd {PROJECT} && python ingestion/load_enrollment_events.py",
    )

    load_sqlserver = BashOperator(
        task_id="load_sqlserver_sources",
        bash_command=f"cd {PROJECT} && python ingestion/load_sqlserver.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT}/healthcare_dbt && dbt run --profiles-dir {PROJECT}/healthcare_dbt",
    )

    generate_csv >> load_pharmacy
    [load_pharmacy, load_enrollment, load_sqlserver] >> dbt_run
