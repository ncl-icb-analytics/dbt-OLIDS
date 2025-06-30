{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All estimated Glomerular Filtration Rate (eGFR) measurements from observations.
*/

WITH base_observations AS (

    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(10,1)) AS egfr_value,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value

    FROM ({{ get_observations("'EGFR_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    egfr_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    original_result_value,

    -- Data quality validation (eGFR typically 5-150+ mL/min/1.73m²)
    CASE
        WHEN egfr_value BETWEEN 1 AND 200 THEN TRUE
        ELSE FALSE
    END AS is_valid_egfr,

    -- CKD stage classification (mL/min/1.73m²)
    CASE
        WHEN egfr_value NOT BETWEEN 1 AND 200 THEN 'Invalid'
        WHEN egfr_value >= 90 THEN 'Normal/High (≥90)'
        WHEN egfr_value >= 60 THEN 'Mild decrease (60-89)'
        WHEN egfr_value >= 45 THEN 'CKD Stage 3a (45-59)'
        WHEN egfr_value >= 30 THEN 'CKD Stage 3b (30-44)'
        WHEN egfr_value >= 15 THEN 'CKD Stage 4 (15-29)'
        WHEN egfr_value < 15 THEN 'CKD Stage 5 (<15)'
        ELSE 'Unknown'
    END AS ckd_stage,

    -- CKD indicator (eGFR < 60 suggests CKD)
    CASE
        WHEN egfr_value < 60 AND egfr_value >= 1 THEN TRUE
        ELSE FALSE
    END AS is_ckd_indicator

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
