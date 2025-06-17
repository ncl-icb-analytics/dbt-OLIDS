{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All diabetes retinal screening programme completions.
Uses RETSCREN_COD cluster which only includes completed screenings 
(excludes declined, unsuitable, or referral codes).
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
        
    FROM ({{ get_observations("'RETSCREN_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    
    -- All records in this model represent completed screenings
    TRUE AS is_completed_screening,
    
    -- Calculate time since screening
    DATEDIFF(day, clinical_effective_date, CURRENT_DATE()) AS days_since_screening,
    
    -- Screening currency flags
    CASE 
        WHEN DATEDIFF(day, clinical_effective_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS screening_current_12m,
    
    CASE 
        WHEN DATEDIFF(day, clinical_effective_date, CURRENT_DATE()) <= 730 THEN TRUE
        ELSE FALSE
    END AS screening_current_24m,
    
    -- Flag retinal screening and diabetic eye screening observations
    CASE WHEN source_cluster_id = 'RETSCREN_COD' THEN TRUE ELSE FALSE END AS is_retinal_screening_code

FROM base_observations
ORDER BY person_id, clinical_effective_date DESC 