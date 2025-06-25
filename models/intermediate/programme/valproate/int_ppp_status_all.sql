{{ config(
    materialized='table',
    description='Intermediate table containing all Pregnancy Prevention Programme (PPP) events from source systems. Raw data collection layer that feeds the PPP dimension table.'
) }}

SELECT
    pp.person_id,
    o.clinical_effective_date::date AS ppp_event_date,
    o.id AS ppp_observation_id,
    mc.concept_code AS ppp_concept_code,
    mc.code_description AS ppp_concept_display,
    CASE
        WHEN vpc.code_category = 'PPP_ENROLLED' THEN 'Yes - PPP enrolled'
        WHEN vpc.code_category = 'PPP_DISCONTINUED' THEN 'No - PPP discontinued'
        WHEN vpc.code_category = 'PPP_NOT_NEEDED' THEN 'No - PPP not needed'
        WHEN vpc.code_category = 'PPP_DECLINED' THEN 'No - PPP declined'
        ELSE 'Unknown PPP status'
    END AS ppp_status_description,
    array_construct(vpc.code_category) AS ppp_categories
FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_codesets_mapped_concepts') }} AS mc
    ON o.observation_core_concept_id = mc.source_code_id
INNER JOIN {{ ref('stg_codesets_valproate_prog_codes') }} AS vpc
    ON mc.concept_code = vpc.code
INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
    ON o.patient_id = pp.patient_id
WHERE vpc.code_category LIKE 'PPP%'
