{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All peripheral arterial disease (PAD) diagnoses from clinical records.
Uses QOF cluster ID PAD_COD for all forms of PAD diagnosis.

Clinical Purpose:
- PAD register inclusion for QOF cardiovascular disease management
- Cardiovascular risk stratification and monitoring
- Secondary prevention pathway identification

QOF Context:
PAD register follows simple diagnosis-only pattern - any PAD diagnosis code 
qualifies for register inclusion. No resolution codes or complex criteria.
This is a lifelong condition register for cardiovascular secondary prevention.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for PAD register and cardiovascular risk models.
*/

WITH base_observations AS (
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- PAD-specific flags
        CASE WHEN obs.cluster_id = 'PAD_COD' THEN TRUE ELSE FALSE END AS is_pad_diagnosis
        
    FROM ({{ get_observations("'PAD_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    SELECT
        person_id,
        
        -- Date aggregates
        MIN(clinical_effective_date) AS earliest_pad_date,
        MAX(clinical_effective_date) AS latest_pad_date,
        COUNT(DISTINCT clinical_effective_date) AS total_pad_episodes,
        
        -- Code arrays for detailed analysis
        ARRAY_AGG(DISTINCT concept_code) AS all_pad_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_pad_concept_displays
        
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
    
    -- PAD-specific flags
    bo.is_pad_diagnosis,
    
    -- Person-level aggregate context
    pa.earliest_pad_date,
    pa.latest_pad_date,
    pa.total_pad_episodes,
    pa.all_pad_concept_codes,
    pa.all_pad_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 