-- Silver: cleaned, typed, deduplicated medical claims.
{{ config(materialized='table') }}

with source as (
    select * from {{ source('bronze', 'bronze_medical_claims') }}
),

cleaned as (
    select
        claim_id,
        member_id,
        provider_id,
        diagnosis_code,
        procedure_code,
        try_to_date(service_date)                as service_date,
        try_cast(billed_amount as number(12,2))  as billed_amount,
        try_cast(allowed_amount as number(12,2)) as allowed_amount,
        claim_status,
        try_to_timestamp(created_at)             as created_at,
        try_to_timestamp(updated_at)             as updated_at,
        _loaded_at
    from source
),

deduped as (
    select *,
        row_number() over (
            partition by claim_id order by updated_at desc
        ) as rn
    from cleaned
)

select
    claim_id, member_id, provider_id, diagnosis_code, procedure_code,
    service_date, billed_amount, allowed_amount, claim_status,
    created_at, updated_at, _loaded_at
from deduped
where rn = 1
