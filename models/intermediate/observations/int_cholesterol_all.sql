{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All total cholesterol measurements from observations.
Includes ALL persons (active, inactive, deceased).
*/

WITH base_observations AS (

    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(6,1)) AS cholesterol_value,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value

    FROM ({{ get_observations("'CHOL2_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    cholesterol_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    original_result_value,

    -- Data quality validation (cholesterol typically 2-15 mmol/L)
    CASE
        WHEN cholesterol_value BETWEEN 0.5 AND 20 THEN TRUE
        ELSE FALSE
    END AS is_valid_cholesterol,

    -- Clinical categorisation (mmol/L)
    CASE
        WHEN cholesterol_value NOT BETWEEN 0.5 AND 20 THEN 'Invalid'
        WHEN cholesterol_value < 5.0 THEN 'Desirable'
        WHEN cholesterol_value < 6.2 THEN 'Borderline High'
        WHEN cholesterol_value >= 6.2 THEN 'High'
        ELSE 'Unknown'
    END AS cholesterol_category

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
