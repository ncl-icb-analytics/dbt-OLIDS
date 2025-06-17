{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All unable-to-have-spirometry observations from clinical records.
Uses QOF cluster ID UNABLESPI_COD for patients unable to perform spirometry tests.

Clinical Purpose:
- COPD register spirometry confirmation requirements (post-April 2023)
- Alternative pathway for COPD register inclusion when spirometry cannot be performed
- Documentation of contraindications or patient inability

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for COPD register spirometry validation.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
        
    FROM ({{ get_observations("'UNABLESPI_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    -- Calculate person-level unable spirometry aggregates
    SELECT
        person_id,
        
        -- Unable spirometry dates
        MIN(clinical_effective_date) AS earliest_unable_spirometry_date,
        MAX(clinical_effective_date) AS latest_unable_spirometry_date,
        COUNT(*) AS total_unable_spirometry_records,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT concept_code) AS all_unable_spirometry_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_unable_spirometry_concept_displays
            
    FROM base_observations
    GROUP BY person_id
)

SELECT 
    bo.person_id,
    bo.observation_id,
    bo.clinical_effective_date,
    bo.concept_code,
    bo.concept_display,
    bo.source_cluster_id,
    
    -- Person-level aggregate context
    pa.earliest_unable_spirometry_date,
    pa.latest_unable_spirometry_date,
    pa.total_unable_spirometry_records,
    
    -- Clinical flags
    TRUE AS is_unable_spirometry_record,
    
    -- QOF context fields
    CASE 
        WHEN pa.latest_unable_spirometry_date >= DATEADD(month, -12, CURRENT_DATE) THEN TRUE
        ELSE FALSE
    END AS has_recent_unable_spirometry,
    
    -- Classification of this specific observation
    'Unable to Perform Spirometry' AS spirometry_observation_type,
    
    -- Traceability arrays
    pa.all_unable_spirometry_concept_codes,
    pa.all_unable_spirometry_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date DESC 