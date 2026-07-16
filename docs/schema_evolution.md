# Schema Evolution Handling

## The scenario

The pharmacy claims CSV source changes schema partway through its history.
Files generated before the cutoff date (2026-07-03) have 9 columns. Files on or
after the cutoff gain a 10th column, `pharmacy_npi`. Both versions must load into
the same bronze table without data loss or corruption.

## How the pipeline absorbs the change

Schema drift is handled in the bronze layer through three deliberate choices:

1. **Tolerant file format.** The CSV file format sets
   `ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE`, so a 9-column file does not fail a
   load into a 10-column table.

2. **Explicit positional mapping.** The COPY statement references source columns
   by position (`$1` through `$10`). For a 9-column file, `$10` resolves to NULL
   rather than shifting later values into the wrong columns. This prevents silent
   data corruption, the most dangerous failure mode in schema evolution.

3. **Untyped bronze.** Every bronze column is `VARCHAR`. No type enforcement means
   unexpected or missing values never break ingestion. Type casting and validation
   are deferred to the silver layer.

## Result

Old and new schema versions coexist in a single bronze table. Pre-cutoff rows show
`NULL` for `pharmacy_npi` (the column did not exist when they were produced);
post-cutoff rows carry the value. Verified with:

```sql
SELECT
    CASE WHEN pharmacy_npi IS NULL THEN 'v1 (no npi)' ELSE 'v2 (has npi)' END AS schema_version,
    COUNT(*) AS row_count
FROM BRONZE.bronze_pharmacy_claims
GROUP BY 1;
```

## Separation of concerns

Bronze *absorbs* the schema change; silver *reconciles* it. Downstream silver
models decide how to treat the new column (for example, backfilling pre-cutoff
records or flagging them), keeping ingestion resilient and business logic
centralized where it belongs.
