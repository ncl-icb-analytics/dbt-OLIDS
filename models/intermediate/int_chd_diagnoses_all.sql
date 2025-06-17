{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All coronary heart disease (CHD) diagnoses from clinical records.
Uses QOF cluster ID CHD_COD for all forms of CHD diagnosis.

Clinical Purpose:
- CHD register inclusion for QOF cardiovascular disease management
- Cardiovascular risk stratification and monitoring
- Secondary prevention pathway identification

QOF Context:
CHD register follows simple diagnosis-only pattern - any CHD diagnosis code 
qualifies for register inclusion. No resolution codes or complex criteria.
This is a lifelong condition register for cardiovascular secondary prevention.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for CHD register and cardiovascular risk models.
*/

WITH base_observations AS (
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- CHD-specific flags
        CASE WHEN obs.cluster_id = 'CHD_COD' THEN TRUE ELSE FALSE END AS is_chd_diagnosis
        
    FROM ({{ get_observations("'CHD_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    SELECT
        person_id,
        
        -- Date aggregates
        MIN(clinical_effective_date) AS earliest_chd_date,
        MAX(clinical_effective_date) AS latest_chd_date,
        COUNT(DISTINCT clinical_effective_date) AS total_chd_episodes,
        
        -- Code arrays for detailed analysis
        ARRAY_AGG(DISTINCT concept_code) AS all_chd_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_chd_concept_displays
        
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
    
    -- CHD-specific flags
    bo.is_chd_diagnosis,
    
    -- Person-level aggregate context
    pa.earliest_chd_date,
    pa.latest_chd_date,
    pa.total_chd_episodes,
    pa.all_chd_concept_codes,
    pa.all_chd_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 