{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All urine Albumin-to-Creatinine Ratio (ACR) measurements from observations.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Uses specific concept codes for ACR tests with clinical validation and CKD staging.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(6,2)) AS acr_value,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        obs.result_value AS original_result_value
        
    FROM ({{ get_observations("'ACR_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    acr_value,
    concept_code,
    concept_display,
    source_cluster_id,
    original_result_value,
    
    -- Data quality validation (ACR typically 0-300+ mg/mmol)
    CASE 
        WHEN acr_value BETWEEN 0 AND 1000 THEN TRUE
        ELSE FALSE
    END AS is_valid_acr,
    
    -- Clinical categorisation (mg/mmol) - CKD risk assessment
    CASE 
        WHEN acr_value NOT BETWEEN 0 AND 1000 THEN 'Invalid'
        WHEN acr_value < 3 THEN 'Normal (<3)'
        WHEN acr_value < 30 THEN 'Mildly Increased (3-30)'
        WHEN acr_value < 300 THEN 'Moderately Increased (30-300)'
        WHEN acr_value >= 300 THEN 'Severely Increased (≥300)'
        ELSE 'Unknown'
    END AS acr_category,
    
    -- CKD indicator based on ACR (≥3 mg/mmol suggests possible kidney damage)
    CASE 
        WHEN acr_value >= 3 AND acr_value <= 1000 THEN TRUE
        ELSE FALSE
    END AS is_acr_elevated,
    
    -- Microalbuminuria indicator (3-30 mg/mmol)
    CASE 
        WHEN acr_value >= 3 AND acr_value < 30 THEN TRUE
        ELSE FALSE
    END AS is_microalbuminuria,
    
    -- Macroalbuminuria indicator (≥30 mg/mmol)
    CASE 
        WHEN acr_value >= 30 AND acr_value <= 1000 THEN TRUE
        ELSE FALSE
    END AS is_macroalbuminuria

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC 