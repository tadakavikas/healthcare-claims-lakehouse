-- Silver: cleaned, typed providers.
{{ config(materialized='table') }}

with source as (
    select * from {{ source('bronze', 'bronze_providers') }}
),

cleaned as (
    select
        provider_id,
        provider_name,
        npi,
        specialty,
        state_code,
        try_to_timestamp(created_at) as created_at,
        try_to_timestamp(updated_at) as updated_at,
        _loaded_at
    from source
),

deduped as (
    select *,
        row_number() over (
            partition by provider_id order by updated_at desc
        ) as rn
    from cleaned
)

select
    provider_id, provider_name, npi, specialty, state_code,
    created_at, updated_at, _loaded_at
from deduped
where rn = 1
