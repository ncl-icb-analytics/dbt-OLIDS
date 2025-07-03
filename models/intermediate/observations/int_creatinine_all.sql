{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Creatinine Observations - All recorded serum creatinine measurements with clinical categorisation.

Clinical Purpose:
• Tracks creatinine levels for kidney function assessment and CKD monitoring
• Supports renal disease management and medication dosing adjustments
• Enables chronic kidney disease case finding and progression monitoring

Data Granularity:
• One row per creatinine observation
• Includes all patients regardless of status (active/inactive/deceased)
• Uses CRE_COD cluster for serum creatinine identification

Key Features:
• Data quality validation with plausible range filtering (20-1000 µmol/L)
• Clinical creatinine categorisation (normal, mildly/moderately/severely elevated)
• Elevated creatinine flags for kidney function screening
• Original value preservation for clinical interpretation'"
        ]
    )
}}

/*
All serum creatinine measurements from observations.
Includes ALL persons (active, inactive, deceased).
*/

WITH base_observations AS (

    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(6,1)) AS creatinine_value,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value

    FROM ({{ get_observations("'CRE_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    creatinine_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    original_result_value,

    -- Data quality validation (creatinine typically 40-400 µmol/L)
    CASE
        WHEN creatinine_value BETWEEN 20 AND 1000 THEN TRUE
        ELSE FALSE
    END AS is_valid_creatinine,

    -- Clinical categorisation (µmol/L) - general adult reference ranges
    CASE
        WHEN creatinine_value NOT BETWEEN 20 AND 1000 THEN 'Invalid'
        WHEN creatinine_value <= 120 THEN 'Normal'
        WHEN creatinine_value <= 200 THEN 'Mildly Elevated'
        WHEN creatinine_value <= 400 THEN 'Moderately Elevated'
        WHEN creatinine_value > 400 THEN 'Severely Elevated'
        ELSE 'Unknown'
    END AS creatinine_category,

    -- Elevated creatinine indicator (>120 µmol/L suggests possible kidney issues)
    CASE
        WHEN creatinine_value > 120 AND creatinine_value <= 1000 THEN TRUE
        ELSE FALSE
    END AS is_elevated_creatinine

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
