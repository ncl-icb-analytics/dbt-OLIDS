{{ config(
    materialized='table',
    description='Intermediate table extracting all neurology-related events for each person, using mapped concepts, observation, and valproate program codes (category NEUROLOGY).') }}

SELECT
    pp.person_id,
    o.clinical_effective_date::date AS neurology_event_date,
    o.id AS neurology_observation_id,
    mc.concept_code AS neurology_concept_code,
    mc.code_description AS neurology_concept_display,
    vpc.code_category AS neurology_code_category
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_codesets_mapped_concepts') }} AS mc
    ON o.observation_source_concept_id = mc.source_code_id
INNER JOIN {{ ref('stg_codesets_valproate_prog_codes') }} AS vpc
    ON mc.concept_code = vpc.code
INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE vpc.code_category = 'NEUROLOGY'
