-- Non-Alcoholic Fatty Liver Disease (NAFLD) diagnosis intermediate model
-- Uses hardcoded SNOMED concept codes as no cluster is currently available
-- This should be updated with proper cluster ID once available in codesets

WITH base_observations AS (
    -- Direct query approach since no cluster is available for NAFLD
    -- Uses hardcoded SNOMED concept codes
    SELECT 
        pp.person_id,
        o.id AS observation_id,
        o.clinical_effective_date::DATE AS clinical_effective_date,
        mc.mapped_concept_code AS concept_code,
        mc.code_description AS concept_display,
        -- Additional fields for consistency
        p.id AS patient_id,
        o.observation_numeric_value AS numeric_value,
        -- NAFLD diagnosis flag (all observations are NAFLD diagnoses)
        TRUE AS is_nafld_diagnosis
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {{ ref('stg_olids_patient_person') }} pp 
        ON o.patient_id = pp.patient_id
    INNER JOIN {{ ref('stg_olids_patient') }} p 
        ON o.patient_id = p.id
    LEFT JOIN {{ ref('stg_codesets_mapped_concepts') }} mc 
        ON o.observation_core_concept_id = mc.source_code_id
    WHERE mc.concept_code IN (
        '197315008',    -- Non-alcoholic fatty liver disease
        '1197739005',   -- NAFLD related code
        '1231824009',   -- NAFLD related code
        '442685003',    -- NAFLD related code
        '722866000',    -- NAFLD related code
        '503681000000108' -- NAFLD related code
    )
    AND o.clinical_effective_date IS NOT NULL
)

SELECT
    person_id,
    patient_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    numeric_value,
    -- Source cluster placeholder (no cluster available)
    'HARDCODED_NAFLD' AS source_cluster_id,
    is_nafld_diagnosis

FROM base_observations
ORDER BY person_id, clinical_effective_date DESC 