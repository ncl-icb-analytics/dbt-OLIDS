{{ config(
    materialized='table',
    description='Intermediate table extracting all psychiatry-related events for each person, using mapped concepts, observation, and valproate program codes (category PSYCH).'
) }}

select
    pp.person_id as person_id,
    o.clinical_effective_date::date as psych_event_date,
    o.id as psych_observation_id,
    mc.concept_code as psych_concept_code,
    mc.code_description as psych_concept_display,
    vpc.code_category as psych_code_category
from {{ ref('stg_olids_observation') }} o
join {{ ref('stg_codesets_mapped_concepts') }} mc
    on o.observation_core_concept_id = mc.source_code_id
join {{ ref('stg_codesets_valproate_prog_codes') }} vpc
    on mc.concept_code = vpc.code
join {{ ref('stg_olids_patient_person') }} pp
    on o.patient_id = pp.patient_id
where vpc.code_category = 'PSYCH'
