{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All rheumatoid arthritis (RA) diagnoses from clinical records.
Uses QOF cluster ID RARTH_COD for rheumatoid arthritis diagnosis codes.

Clinical Purpose:
- RA register inclusion for QOF quality measures
- Rheumatoid arthritis disease management monitoring
- DMARDs (Disease-Modifying Anti-Rheumatic Drugs) prescribing support

QOF Context:
RA register follows simple diagnosis-only pattern with age restriction - any RA diagnosis 
for patients aged 16+ qualifies for register inclusion. No resolution codes.
This supports RA quality measures and specialist care monitoring.

Note: QOF RA register requires age ≥16 years at diagnosis.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for RA register (with age filtering applied in fact layer).
*/

WITH base_observations AS (
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Rheumatoid arthritis-specific flags
        CASE WHEN obs.cluster_id = 'RARTH_COD' THEN TRUE ELSE FALSE END AS is_ra_diagnosis
        
    FROM ({{ get_observations("'RARTH_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    SELECT
        person_id,
        
        -- Date aggregates
        MIN(clinical_effective_date) AS earliest_ra_date,
        MAX(clinical_effective_date) AS latest_ra_date,
        COUNT(DISTINCT clinical_effective_date) AS total_ra_episodes,
        
        -- Code arrays for detailed analysis
        ARRAY_AGG(DISTINCT concept_code) AS all_ra_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_ra_concept_displays
        
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
    
    -- Rheumatoid arthritis-specific flags
    bo.is_ra_diagnosis,
    
    -- Person-level aggregate context
    pa.earliest_ra_date,
    pa.latest_ra_date,
    pa.total_ra_episodes,
    pa.all_ra_concept_codes,
    pa.all_ra_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 