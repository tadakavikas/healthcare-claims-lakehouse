-- Silver: cleaned, typed members. CDC merge in bronze already keeps one row
-- per member_id, but we dedup defensively and cast types here.
{{ config(materialized='table') }}

with source as (
    select * from {{ source('bronze', 'bronze_members') }}
),

cleaned as (
    select
        member_id,
        first_name,
        last_name,
        try_to_date(date_of_birth)  as date_of_birth,
        gender,
        plan_code,
        state_code,
        try_to_date(effective_date) as effective_date,
        try_to_timestamp(created_at) as created_at,
        try_to_timestamp(updated_at) as updated_at,
        _loaded_at
    from source
),

deduped as (
    select *,
        row_number() over (
            partition by member_id order by updated_at desc
        ) as rn
    from cleaned
)

select
    member_id, first_name, last_name, date_of_birth, gender,
    plan_code, state_code, effective_date, created_at, updated_at, _loaded_at
from deduped
where rn = 1
