{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All hypertension diagnosis observations from clinical records.
Uses QOF hypertension cluster IDs:
- HYP_COD: Hypertension diagnoses
- HYPRES_COD: Hypertension resolved/remission codes

Clinical Purpose:
- QOF hypertension register data collection
- Blood pressure management monitoring
- Cardiovascular risk assessment support
- Resolution status tracking

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_hypertension_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag different types of hypertension codes following QOF definitions
        CASE WHEN obs.source_cluster_id = 'HYP_COD' THEN TRUE ELSE FALSE END AS is_hypertension_diagnosis_code,
        CASE WHEN obs.source_cluster_id = 'HYPRES_COD' THEN TRUE ELSE FALSE END AS is_hypertension_resolved_code
        
    FROM {{ get_observations("'HYP_COD', 'HYPRES_COD'") }} obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    -- Calculate person-level hypertension date aggregates for context
    SELECT
        person_id,
        
        -- Hypertension diagnosis dates
        MIN(CASE WHEN is_hypertension_diagnosis_code THEN clinical_effective_date END) AS earliest_hypertension_date,
        MAX(CASE WHEN is_hypertension_diagnosis_code THEN clinical_effective_date END) AS latest_hypertension_date,
        
        -- Resolution dates
        MIN(CASE WHEN is_hypertension_resolved_code THEN clinical_effective_date END) AS earliest_resolved_date,
        MAX(CASE WHEN is_hypertension_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_hypertension_diagnosis_code THEN concept_code END) 
            FILTER (WHERE is_hypertension_diagnosis_code) AS all_hypertension_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_hypertension_diagnosis_code THEN concept_display END) 
            FILTER (WHERE is_hypertension_diagnosis_code) AS all_hypertension_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_hypertension_resolved_code THEN concept_code END) 
            FILTER (WHERE is_hypertension_resolved_code) AS all_resolved_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_hypertension_resolved_code THEN concept_display END) 
            FILTER (WHERE is_hypertension_resolved_code) AS all_resolved_concept_displays
            
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
    
    -- Hypertension type flags
    bo.is_hypertension_diagnosis_code,
    bo.is_hypertension_resolved_code,
    
    -- Person-level aggregate context (for downstream QOF logic)
    pa.earliest_hypertension_date,
    pa.latest_hypertension_date,
    pa.earliest_resolved_date,
    pa.latest_resolved_date,
    
    -- QOF-specific derived fields
    CASE 
        WHEN pa.latest_resolved_date IS NULL THEN FALSE
        WHEN pa.latest_hypertension_date > pa.latest_resolved_date THEN FALSE
        ELSE TRUE
    END AS is_hypertension_currently_resolved,
    
    -- Hypertension observation type determination
    CASE
        WHEN bo.is_hypertension_diagnosis_code THEN 'Hypertension Diagnosis'
        WHEN bo.is_hypertension_resolved_code THEN 'Hypertension Resolved'
        ELSE 'Unknown'
    END AS hypertension_observation_type,
    
    -- QOF register eligibility context (basic)
    CASE
        WHEN pa.latest_hypertension_date IS NOT NULL 
             AND (pa.latest_resolved_date IS NULL OR pa.latest_hypertension_date > pa.latest_resolved_date)
        THEN TRUE
        ELSE FALSE
    END AS has_active_hypertension_diagnosis,
    
    -- Traceability arrays
    pa.all_hypertension_concept_codes,
    pa.all_hypertension_concept_displays,
    pa.all_resolved_concept_codes,
    pa.all_resolved_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 