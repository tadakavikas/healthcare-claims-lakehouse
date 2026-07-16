-- Silver: cleaned, typed, deduplicated pharmacy claims.
-- Reads raw VARCHAR bronze and casts to proper types. The schema-evolution
-- column (pharmacy_npi) is reconciled here: pre-cutoff rows get 'UNKNOWN'.

{{ config(materialized='table') }}

with source as (

    select * from {{ source('bronze', 'bronze_pharmacy_claims') }}

),

cleaned as (

    select
        claim_id,
        member_id,
        ndc_code,
        drug_name,
        try_cast(quantity as integer)             as quantity,
        try_cast(days_supply as integer)          as days_supply,
        try_to_timestamp(fill_time)               as fill_time,
        try_cast(ingredient_cost as number(10,2)) as ingredient_cost,
        try_cast(copay_amount as number(10,2))    as copay_amount,
        -- reconcile schema evolution: old files had no npi
        coalesce(pharmacy_npi, 'UNKNOWN')         as pharmacy_npi,
        _source_file,
        _loaded_at
    from source

),

deduped as (

    -- keep one row per claim_id (source has intentional duplicates)
    select *,
        row_number() over (
            partition by claim_id
            order by _loaded_at desc
        ) as rn
    from cleaned

)

select
    claim_id,
    member_id,
    ndc_code,
    drug_name,
    quantity,
    days_supply,
    fill_time,
    ingredient_cost,
    copay_amount,
    pharmacy_npi,
    _source_file,
    _loaded_at
from deduped
where rn = 1
