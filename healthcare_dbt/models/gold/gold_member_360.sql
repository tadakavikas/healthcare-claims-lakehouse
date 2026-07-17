-- Gold: one row per member summarizing all their activity across sources.
{{ config(materialized='table') }}

with members as (
    select * from {{ ref('silver_members') }}
),

claim_stats as (
    select
        member_id,
        count(*)            as total_claims,
        sum(billed_amount)  as total_billed,
        sum(allowed_amount) as total_allowed,
        max(service_date)   as last_service_date
    from {{ ref('silver_medical_claims') }}
    group by member_id
),

pharmacy_stats as (
    select
        member_id,
        count(*)             as total_rx_fills,
        sum(ingredient_cost) as total_rx_cost
    from {{ ref('silver_pharmacy_claims') }}
    group by member_id
),

enrollment_stats as (
    select
        member_id,
        count(*)        as total_enrollment_events,
        max(event_time) as last_enrollment_event
    from {{ ref('silver_enrollment_events') }}
    group by member_id
)

select
    m.member_id,
    m.first_name,
    m.last_name,
    m.plan_code,
    m.state_code,
    m.date_of_birth,
    coalesce(cs.total_claims, 0)             as total_claims,
    coalesce(cs.total_billed, 0)            as total_billed,
    coalesce(cs.total_allowed, 0)           as total_allowed,
    cs.last_service_date,
    coalesce(ps.total_rx_fills, 0)          as total_rx_fills,
    coalesce(ps.total_rx_cost, 0)           as total_rx_cost,
    coalesce(es.total_enrollment_events, 0) as total_enrollment_events,
    es.last_enrollment_event
from members m
left join claim_stats cs      on m.member_id = cs.member_id
left join pharmacy_stats ps   on m.member_id = ps.member_id
left join enrollment_stats es on m.member_id = es.member_id
