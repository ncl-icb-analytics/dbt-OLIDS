{{ config(
    materialized='table',
    description='Intermediate table extracting all ARAF referral-related events for each person, using mapped concepts, observation, and valproate program codes (category REFERRAL).'
) }}

SELECT
    pp.person_id,
    o.clinical_effective_date::date AS araf_referral_event_date,
    o.id AS araf_referral_observation_id,
    mc.concept_code AS araf_referral_concept_code,
    mc.code_description AS araf_referral_concept_display,
    vpc.code_category AS araf_referral_code_category
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_codesets_mapped_concepts') }} AS mc
    ON o.observation_core_concept_id = mc.source_code_id
INNER JOIN {{ ref('stg_codesets_valproate_prog_codes') }} AS vpc
    ON mc.concept_code = vpc.code
INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE vpc.code_category = 'REFERRAL'
