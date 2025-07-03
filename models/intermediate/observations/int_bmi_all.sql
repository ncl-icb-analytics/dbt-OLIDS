{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: BMI Observations - All recorded BMI measurements with clinical categorisation.

Clinical Purpose:
• Tracks BMI measurements for obesity management and health monitoring
• Supports weight management programmes and QOF reporting
• Enables longitudinal analysis of BMI trends and clinical outcomes

Data Granularity:
• One row per BMI observation
• Includes all patients regardless of status (active/inactive/deceased)
• Uses BMIVAL_COD cluster for BMI value identification

Key Features:
• Data quality validation with plausible range filtering (5-400)
• Clinical BMI categorisation (underweight, normal, overweight, obese classes)
• Original value preservation for audit and quality assurance
• Comprehensive observation metadata for traceability'"
        ]
    )
}}

/*
All numeric BMI measurements from observations.
Includes ALL persons (active, inactive, deceased)
using BMIVAL_COD with basic validation (5-400 range).
*/

WITH base_observations AS (
    -- Now using simplified macro that should work without duplicates
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(10,2)) AS bmi_value,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value

    FROM ({{ get_observations("'BMIVAL_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    bmi_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    original_result_value,

    -- Data quality validation
    CASE
        WHEN bmi_value BETWEEN 5 AND 400 THEN TRUE
        ELSE FALSE
    END AS is_valid_bmi,

    -- Clinical categorisation (only for valid BMI)
    CASE
        WHEN bmi_value NOT BETWEEN 5 AND 400 THEN 'Invalid'
        WHEN bmi_value < 18.5 THEN 'Underweight'
        WHEN bmi_value < 25 THEN 'Normal'
        WHEN bmi_value < 30 THEN 'Overweight'
        WHEN bmi_value < 35 THEN 'Obese Class I'
        WHEN bmi_value < 40 THEN 'Obese Class II'
        ELSE 'Obese Class III'
    END AS bmi_category

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
