{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All Non-Alcoholic Fatty Liver Disease (NAFLD) diagnosis observations from clinical records.
Currently uses hardcoded SNOMED concept codes as no cluster is available in REFERENCE.

⚠️ TODO: Update with proper cluster ID once NAFLD_COD becomes available in REFERENCE.

Clinical Purpose:
- NAFLD diagnosis tracking
- Liver health assessment
- Potential QOF register development

Note: This should be updated to use get_observations() macro once proper cluster ID is available.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per NAFLD observation.
Use this model as input for fct_person_nafld_register.sql which applies business rules.
*/

SELECT
    o.id AS observation_id,
    pp.person_id,
    o.clinical_effective_date::DATE AS clinical_effective_date,
    mc.concept_code,
    mc.code_description AS concept_display,

    -- Source information
    'HARDCODED_NAFLD' AS source_cluster_id,
    p.id AS patient_id,

    -- NAFLD-specific flags (observation-level only)
    TRUE AS is_nafld_diagnosis_code,

    -- Observation type determination
    'NAFLD Diagnosis' AS nafld_observation_type,

    -- Additional clinical context
    (o.result_value)::NUMBER(10, 2) AS numeric_value

FROM {{ ref('stg_olids_observation') }} AS o
INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
    ON o.patient_id = pp.patient_id
INNER JOIN {{ ref('stg_olids_patient') }} AS p
    ON o.patient_id = p.id
LEFT JOIN {{ ref('stg_codesets_mapped_concepts') }} AS mc
    ON o.observation_source_concept_id = mc.source_code_id
WHERE
    mc.concept_code IN (
        '197315008',    -- Non-alcoholic fatty liver disease
        '1197739005',   -- NAFLD related code
        '1231824009',   -- NAFLD related code
        '442685003',    -- NAFLD related code
        '722866000',    -- NAFLD related code
        '503681000000108' -- NAFLD related code
    )
    AND o.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
