{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All waist circumference measurements from observations.
*/

WITH base_observations AS (

    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(10,2)) AS waist_circumference_value,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value

    FROM ({{ get_observations("'WAIST_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
      AND REGEXP_LIKE(obs.result_value::VARCHAR, '^[+-]?([0-9]*[.])?[0-9]+$') -- Ensure numeric
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    waist_circumference_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    original_result_value,

    -- Data quality validation (waist circumference typically 50-200 cm)
    CASE
        WHEN waist_circumference_value BETWEEN 30 AND 250 THEN TRUE
        ELSE FALSE
    END AS is_valid_waist_circumference,

    -- Clinical risk categorisation (cm) - general adult guidelines
    CASE
        WHEN waist_circumference_value NOT BETWEEN 30 AND 250 THEN 'Invalid'
        WHEN waist_circumference_value < 80 THEN 'Low Risk'
        WHEN waist_circumference_value < 88 THEN 'Moderate Risk (Female)'
        WHEN waist_circumference_value < 94 THEN 'Moderate Risk'
        WHEN waist_circumference_value < 102 THEN 'High Risk'
        WHEN waist_circumference_value >= 102 THEN 'Very High Risk'
        ELSE 'Unknown'
    END AS waist_risk_category,

    -- High risk indicator (≥88cm for women, ≥102cm for men - using higher threshold)
    CASE
        WHEN waist_circumference_value >= 88 AND waist_circumference_value <= 250 THEN TRUE
        ELSE FALSE
    END AS is_high_waist_risk,

    -- Very high risk indicator (≥102cm)
    CASE
        WHEN waist_circumference_value >= 102 AND waist_circumference_value <= 250 THEN TRUE
        ELSE FALSE
    END AS is_very_high_waist_risk

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
