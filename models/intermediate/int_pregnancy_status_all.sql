{{
    config(
        materialized='table'
    )
}}

-- Pregnancy Status Intermediate Model (Data Collection Layer)
-- Collects ALL pregnancy-related observations using standardised pattern
-- Single Responsibility: Pregnancy observation data collection only

WITH pregnancy_observations AS (
    -- Use our standard macro for pregnancy observations
    SELECT 
        observation_id,
        person_id,
        clinical_effective_date,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_display,
        cluster_id AS source_cluster_id
    FROM ({{ get_observations("'PREG_COD', 'PREGDEL_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
)

-- Additional transformations for pregnancy-specific flags
SELECT 
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    
    -- Pregnancy-specific flags (observation-level)
    CASE WHEN source_cluster_id = 'PREG_COD' THEN TRUE ELSE FALSE END AS is_pregnancy_code,
    CASE WHEN source_cluster_id = 'PREGDEL_COD' THEN TRUE ELSE FALSE END AS is_delivery_code
    
FROM pregnancy_observations
ORDER BY person_id, clinical_effective_date DESC 