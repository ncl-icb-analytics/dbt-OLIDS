-- depends_on: {{ ref('stable_patient') }}

{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['id'],
        alias='person',
        incremental_strategy='merge',
        tags=['stable', 'incremental']
    )
}}

select distinct
    id,
    nhs_number_hash,
    title,
    gender_concept_id,
    birth_year,
    birth_month,
    death_year,
    death_month
from {{ ref('base_olids_person') }}

-- Note: Person is derived from patient, so incremental logic based on patient changes
{% if is_incremental() %}
where id in (
    select distinct id
    from {{ ref('stable_patient') }}
    where lds_start_date_time > (
        select coalesce(max(lds_start_date_time), '1900-01-01'::timestamp)
        from {{ ref('stable_patient') }}
        where id in (select id from {{ this }})
    )
)
{% endif %}