-- Non-Alcoholic Fatty Liver Disease (NAFLD) diagnosis intermediate model
-- Uses hardcoded SNOMED concept codes as no cluster is currently available
-- This should be updated with proper cluster ID once available in codesets

WITH base_observations AS (
    SELECT 
        obs.person_id,
        obs.observation_id,
        obs.clinical_effective_date,
        obs.concept_code,
        obs.concept_display,
        -- NAFLD diagnosis flag (all observations are NAFLD diagnoses)
        TRUE AS is_nafld_diagnosis
    FROM {{ get_observations() }} obs
    WHERE obs.concept_code IN (
        '197315008',    -- Non-alcoholic fatty liver disease
        '1197739005',   -- NAFLD related code
        '1231824009',   -- NAFLD related code
        '442685003',    -- NAFLD related code
        '722866000',    -- NAFLD related code
        '503681000000108' -- NAFLD related code
    )
    AND obs.clinical_effective_date IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    -- Source cluster placeholder (no cluster available)
    'HARDCODED_NAFLD' AS source_cluster_id,
    is_nafld_diagnosis

FROM base_observations
ORDER BY person_id, clinical_effective_date DESC 