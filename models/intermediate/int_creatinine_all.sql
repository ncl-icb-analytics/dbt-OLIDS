{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All serum creatinine measurements from observations.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Simple pattern using CRE_COD with clinical validation and reference ranges.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(6,1)) AS creatinine_value,
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