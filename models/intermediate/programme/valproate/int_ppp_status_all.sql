{{ config(
    materialized='table',
    description='Intermediate table containing all Pregnancy Prevention Programme (PPP) events from source systems. Raw data collection layer that feeds the PPP dimension table.'
) }}

select
    pp.person_id as person_id,
    o.clinical_effective_date::date as ppp_event_date,
    o.id as ppp_observation_id,
    mc.concept_code as ppp_concept_code,
    mc.code_description as ppp_concept_display,
    case 
        when vpc.code_category = 'PPP_ENROLLED' then 'Yes - PPP enrolled'
        when vpc.code_category = 'PPP_DISCONTINUED' then 'No - PPP discontinued'
        when vpc.code_category = 'PPP_NOT_NEEDED' then 'No - PPP not needed'
        when vpc.code_category = 'PPP_DECLINED' then 'No - PPP declined'
        else 'Unknown PPP status'
    end as ppp_status_description,
    array_construct(vpc.code_category) as ppp_categories
from {{ ref('stg_olids_observation') }} o
join {{ ref('stg_codesets_mapped_concepts') }} mc
    on o.observation_core_concept_id = mc.source_code_id
join {{ ref('stg_codesets_valproate_prog_codes') }} vpc
    on mc.concept_code = vpc.code
join {{ ref('stg_olids_patient_person') }} pp
    on o.patient_id = pp.patient_id
where vpc.code_category like 'PPP%'
