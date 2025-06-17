{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All familial hypercholesterolaemia (FH) diagnoses from clinical records.
Uses QOF cluster ID FHYP_COD for familial hypercholesterolaemia diagnosis codes.

Clinical Purpose:
- FH register inclusion for QOF quality measures
- Familial hypercholesterolaemia cascade screening
- High-intensity statin therapy monitoring

QOF Context:
FH register follows simple diagnosis-only pattern - any FH diagnosis qualifies for 
register inclusion. No resolution codes or complex criteria.
This supports FH quality measures and cascade family screening programmes.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for FH register.
*/

WITH base_observations AS (
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Familial hypercholesterolaemia-specific flags
        CASE WHEN obs.cluster_id = 'FHYP_COD' THEN TRUE ELSE FALSE END AS is_fhyp_diagnosis
        
    FROM ({{ get_observations("'FHYP_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    SELECT
        person_id,
        
        -- Date aggregates
        MIN(clinical_effective_date) AS earliest_fhyp_date,
        MAX(clinical_effective_date) AS latest_fhyp_date,
        COUNT(DISTINCT clinical_effective_date) AS total_fhyp_episodes,
        
        -- Code arrays for detailed analysis
        ARRAY_AGG(DISTINCT concept_code) AS all_fhyp_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_fhyp_concept_displays
        
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
    
    -- Familial hypercholesterolaemia-specific flags
    bo.is_fhyp_diagnosis,
    
    -- Person-level aggregate context
    pa.earliest_fhyp_date,
    pa.latest_fhyp_date,
    pa.total_fhyp_episodes,
    pa.all_fhyp_concept_codes,
    pa.all_fhyp_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 