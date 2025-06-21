{{ config(
    materialized='table',
    description='Intermediate table extracting all ARAF-related events for each person, using mapped concepts, observation, and valproate program codes. Applies lookback logic as defined in valproate program codes.'
) }}

select
    pp.person_id as person_id,
    o.clinical_effective_date::date as araf_event_date,
    o.id as araf_observation_id,
    mc.concept_code as araf_concept_code,
    mc.code_description as araf_concept_display,
    vpc.code_category as araf_code_category,
    case when vpc.code_category = 'ARAF' then true else false end as is_specific_araf_form_code
from {{ ref('stg_olids_observation') }} o
join {{ ref('stg_codesets_mapped_concepts') }} mc
    on o.observation_core_concept_id = mc.source_code_id
join {{ ref('stg_codesets_valproate_prog_codes') }} vpc
    on mc.concept_code = vpc.code
join {{ ref('stg_olids_patient_person') }} pp
    on o.patient_id = pp.patient_id
where vpc.code_category = 'ARAF'
  and (
    vpc.lookback_years_offset is null
    or o.clinical_effective_date::date >= dateadd(year, vpc.lookback_years_offset, current_date())
)
