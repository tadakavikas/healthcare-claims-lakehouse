-- Silver: cleaned, typed, deduplicated enrollment events.
{{ config(materialized='table') }}

with source as (
    select * from {{ source('bronze', 'bronze_enrollment_events') }}
),

cleaned as (
    select
        event_id,
        member_id,
        event_type,
        plan_code,
        try_to_timestamp(event_time) as event_time,
        try_to_timestamp(created_at) as created_at,
        _loaded_at
    from source
),

deduped as (
    select *,
        row_number() over (
            partition by event_id order by created_at desc
        ) as rn
    from cleaned
)

select
    event_id, member_id, event_type, plan_code,
    event_time, created_at, _loaded_at
from deduped
where rn = 1
