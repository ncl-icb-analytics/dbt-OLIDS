{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All COPD diagnosis observations from clinical records.
Uses QOF COPD cluster IDs:
- COPD_COD: COPD diagnoses
- COPDRES_COD: COPD resolved/remission codes

Clinical Purpose:
- QOF COPD register data collection
- COPD spirometry confirmation requirements (post-April 2023)
- Respiratory management monitoring
- Resolution status tracking

Key QOF Requirements:
- Pre-April 2023: Diagnosis alone sufficient for register
- Post-April 2023: Requires spirometry confirmation (FEV1/FVC <0.7) OR unable-to-have-spirometry status

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_copd_register.sql which applies QOF business rules and spirometry requirements.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Flag different types of COPD codes following QOF definitions
        CASE WHEN obs.cluster_id = 'COPD_COD' THEN TRUE ELSE FALSE END AS is_copd_diagnosis_code,
        CASE WHEN obs.cluster_id = 'COPDRES_COD' THEN TRUE ELSE FALSE END AS is_copd_resolved_code
        
    FROM ({{ get_observations("'COPD_COD', 'COPDRES_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    -- Calculate person-level COPD date aggregates for context
    SELECT
        person_id,
        
        -- COPD diagnosis dates
        MIN(CASE WHEN is_copd_diagnosis_code THEN clinical_effective_date END) AS earliest_copd_date,
        MAX(CASE WHEN is_copd_diagnosis_code THEN clinical_effective_date END) AS latest_copd_date,
        
        -- Resolution dates
        MIN(CASE WHEN is_copd_resolved_code THEN clinical_effective_date END) AS earliest_resolved_date,
        MAX(CASE WHEN is_copd_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- QOF-specific unresolved diagnosis logic (basic version - complex logic in fact layer)
        MIN(CASE WHEN is_copd_diagnosis_code THEN clinical_effective_date END) AS earliest_unresolved_diagnosis_date,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_copd_diagnosis_code THEN concept_code ELSE NULL END) AS all_copd_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_copd_diagnosis_code THEN concept_display ELSE NULL END) AS all_copd_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_copd_resolved_code THEN concept_code ELSE NULL END) AS all_resolved_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_copd_resolved_code THEN concept_display ELSE NULL END) AS all_resolved_concept_displays
            
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
    
    -- COPD type flags
    bo.is_copd_diagnosis_code,
    bo.is_copd_resolved_code,
    
    -- Person-level aggregate context (for downstream QOF logic)
    pa.earliest_copd_date,
    pa.latest_copd_date,
    pa.earliest_resolved_date,
    pa.latest_resolved_date,
    pa.earliest_unresolved_diagnosis_date,
    
    -- QOF-specific derived fields
    CASE 
        WHEN pa.latest_resolved_date IS NULL THEN FALSE
        WHEN pa.latest_copd_date > pa.latest_resolved_date THEN FALSE
        ELSE TRUE
    END AS is_copd_currently_resolved,
    
    -- COPD observation type determination
    CASE
        WHEN bo.is_copd_diagnosis_code THEN 'COPD Diagnosis'
        WHEN bo.is_copd_resolved_code THEN 'COPD Resolved'
        ELSE 'Unknown'
    END AS copd_observation_type,
    
    -- QOF register eligibility context (basic - needs spirometry validation in fact layer)
    CASE
        WHEN pa.earliest_unresolved_diagnosis_date IS NOT NULL
        THEN TRUE
        ELSE FALSE
    END AS has_unresolved_copd_diagnosis,
    
    -- QOF temporal rules for spirometry requirements
    CASE
        WHEN pa.earliest_unresolved_diagnosis_date IS NULL THEN FALSE
        WHEN pa.earliest_unresolved_diagnosis_date < '2023-04-01' THEN TRUE  -- Pre-April 2023 rules
        ELSE FALSE  -- Post-April 2023 requires spirometry confirmation (handled in fact layer)
    END AS meets_pre_april_2023_criteria,
    
    CASE
        WHEN pa.earliest_unresolved_diagnosis_date IS NULL THEN FALSE
        WHEN pa.earliest_unresolved_diagnosis_date >= '2023-04-01' THEN TRUE  -- Needs spirometry validation
        ELSE FALSE
    END AS requires_spirometry_confirmation,
    
    -- Traceability arrays
    pa.all_copd_concept_codes,
    pa.all_copd_concept_displays,
    pa.all_resolved_concept_codes,
    pa.all_resolved_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 