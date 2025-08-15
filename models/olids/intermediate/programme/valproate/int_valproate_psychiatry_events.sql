{{ config(
    materialized='table',
    description='Intermediate table extracting all psychiatry-related events for each person, using mapped concepts, observation, and valproate program codes (category PSYCH).') }}

SELECT
    pp.person_id,
    o.clinical_effective_date::date AS psych_event_date,
    o.id AS psych_observation_id,
    mc.concept_code AS psych_concept_code,
    mc.code_description AS psych_concept_display,
    vpc.code_category AS psych_code_category
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_reference_mapped_concepts') }} AS mc
    ON o.observation_source_concept_id = mc.source_code_id
INNER JOIN {{ ref('stg_reference_valproate_prog_codes') }} AS vpc
    ON mc.concept_code = vpc.code
INNER JOIN {{ ref('int_patient_person_unique') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE vpc.code_category = 'PSYCH'
