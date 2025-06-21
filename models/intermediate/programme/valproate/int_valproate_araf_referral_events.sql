{{ config(
    materialized='table',
    description='Intermediate table extracting all ARAF referral-related events for each person, using mapped concepts, observation, and valproate program codes (category REFERRAL).'
) }}

select
    pp.person_id as person_id,
    o.clinical_effective_date::date as araf_referral_event_date,
    o.id as araf_referral_observation_id,
    mc.concept_code as araf_referral_concept_code,
    mc.code_description as araf_referral_concept_display,
    vpc.code_category as araf_referral_code_category
from {{ ref('stg_olids_observation') }} o
join {{ ref('stg_codesets_mapped_concepts') }} mc
    on o.observation_core_concept_id = mc.source_code_id
join {{ ref('stg_codesets_valproate_prog_codes') }} vpc
    on mc.concept_code = vpc.code
join {{ ref('stg_olids_patient_person') }} pp
    on o.patient_id = pp.patient_id
where vpc.code_category = 'REFERRAL'
