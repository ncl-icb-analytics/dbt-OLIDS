{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All numeric BMI measurements from observations.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Simple pattern using only BMIVAL_COD with basic validation (5-400 range).
Matches legacy structure with result_unit_display field.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.sk_patient_id,
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
    sk_patient_id,
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