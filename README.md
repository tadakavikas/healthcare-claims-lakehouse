# Healthcare Claims Lakehouse

End-to-end data engineering project: 4 simulated source systems ingested into
Snowflake through idempotent, watermark-driven pipelines, modeled with a
medallion architecture (bronze / silver / gold), orchestrated hourly with Airflow.

## Architecture

```
 SOURCES (simulated, Docker)                INGESTION                WAREHOUSE (Snowflake)
 ┌─────────────────────────┐
 │ 1. Pharmacy claims CSVs │──hourly──┐
 │    (vendor file drop)   │          │
 ├─────────────────────────┤          │   ┌──────────────┐   ┌────────┐  ┌────────┐  ┌──────┐
 │ 2. Enrollment events API│──cursor──┼──▶│ Python + SQL │──▶│ BRONZE │─▶│ SILVER │─▶│ GOLD │
 │    (paginated REST)     │          │   │ (idempotent, │   │  raw   │  │ clean  │  │ marts│
 ├─────────────────────────┤          │   │  watermarks) │   └────────┘  └────────┘  └──────┘
 │ 3. SQL Server: claims   │──10 min──┤   └──────────────┘        dbt transforms
 │ 4. SQL Server: members  │──10 min──┘        ▲
 └─────────────────────────┘                   │
                                     Airflow (hourly schedule + backfill)
```

## Deliberate failure modes baked into the sources

| Problem | Where | Solved in |
|---|---|---|
| Duplicate records | CSV + API | Idempotent MERGE / dedup keys |
| Late-arriving data | API events arrive up to 6h late | Watermark with lookback window |
| Schema evolution | CSV gains a new column after a cutoff date | Schema drift handling in bronze |

## Quick start

```bash
# 1. Environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# 2. Start source systems (2x SQL Server, API, insert jobs)
docker compose up -d

# 3. Generate hourly CSV files
python data_sources/csv_generator/generate_hourly_csv.py --hours 24

# 4. Hit the API
curl "http://localhost:8000/api/v1/enrollment-events?since=2026-07-01T00:00:00"

# 5. Query SQL Server
docker exec -it sqlserver-claims /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C \
  -Q "SELECT TOP 5 * FROM ClaimsDB.dbo.medical_claims"
```

## Project phases

- [x] Phase 0: Scaffold + Docker environment
- [x] Phase 1: Four simulated data sources
- [ ] Phase 2: Snowflake ingestion (stages, COPY INTO)
- [ ] Phase 3: Idempotency, watermarks, backfill
- [ ] Phase 4: Schema evolution handling
- [ ] Phase 5: Airflow orchestration
- [ ] Phase 6: dbt medallion model
- [ ] Phase 7: 10x volume load test
