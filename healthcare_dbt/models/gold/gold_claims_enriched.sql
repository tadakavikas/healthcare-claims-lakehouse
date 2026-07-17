-- Gold: medical claims enriched with member and provider context.
-- One row per claim, with the cryptic IDs resolved into real attributes
-- an analyst can filter and group by (plan, state, specialty, etc).
{{ config(materialized='table') }}

with claims as (
    select * from {{ ref('silver_medical_claims') }}
),

members as (
    select member_id, plan_code, state_code as member_state, gender, date_of_birth
    from {{ ref('silver_members') }}
),

providers as (
    select provider_id, provider_name, specialty, state_code as provider_state
    from {{ ref('silver_providers') }}
)

select
    c.claim_id,
    c.member_id,
    c.provider_id,
    c.diagnosis_code,
    c.procedure_code,
    c.service_date,
    c.billed_amount,
    c.allowed_amount,
    c.billed_amount - c.allowed_amount as disallowed_amount,
    c.claim_status,
    m.plan_code,
    m.member_state,
    m.gender,
    p.provider_name,
    p.specialty,
    p.provider_state,
    case when m.member_state = p.provider_state then true else false end as in_state_care,
    c.updated_at
from claims c
left join members m   on c.member_id = m.member_id
left join providers p on c.provider_id = p.provider_id
