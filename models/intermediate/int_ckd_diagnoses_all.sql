{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All chronic kidney disease (CKD) diagnosis observations from clinical records.
Uses QOF CKD cluster IDs:
- CKD_COD: CKD diagnoses
- CKDRES_COD: CKD resolved/remission codes

Clinical Purpose:
- QOF CKD register data collection (aged 18+, unresolved CKD diagnosis)
- CKD staging and classification support
- Kidney function monitoring
- Resolution status tracking

Key QOF Requirements:
- Register inclusion: Age ≥18, latest unresolved CKD diagnosis
- Lab correlation with eGFR and ACR values
- CKD stage determination support
- Laboratory confirmation tracking

Note: This model provides diagnosis codes only. Lab-based CKD inference 
(eGFR/ACR staging) is handled separately in intermediate_ckd_lab_inference.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_ckd_register.sql which applies QOF business rules and lab integration.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Flag different types of CKD codes following QOF definitions
        CASE WHEN obs.cluster_id = 'CKD_COD' THEN TRUE ELSE FALSE END AS is_ckd_diagnosis_code,
        CASE WHEN obs.cluster_id = 'CKDRES_COD' THEN TRUE ELSE FALSE END AS is_ckd_resolved_code
        
    FROM ({{ get_observations("'CKD_COD', 'CKDRES_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    -- Calculate person-level CKD date aggregates for context
    SELECT
        person_id,
        
        -- CKD diagnosis dates
        MIN(CASE WHEN is_ckd_diagnosis_code THEN clinical_effective_date END) AS earliest_ckd_date,
        MAX(CASE WHEN is_ckd_diagnosis_code THEN clinical_effective_date END) AS latest_ckd_date,
        
        -- Resolution dates
        MIN(CASE WHEN is_ckd_resolved_code THEN clinical_effective_date END) AS earliest_resolved_date,
        MAX(CASE WHEN is_ckd_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_ckd_diagnosis_code THEN concept_code ELSE NULL END) AS all_ckd_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_ckd_diagnosis_code THEN concept_display ELSE NULL END) AS all_ckd_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_ckd_resolved_code THEN concept_code ELSE NULL END) AS all_resolved_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_ckd_resolved_code THEN concept_display ELSE NULL END) AS all_resolved_concept_displays
            
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
    
    -- CKD type flags
    bo.is_ckd_diagnosis_code,
    bo.is_ckd_resolved_code,
    
    -- Person-level aggregate context (for downstream QOF logic)
    pa.earliest_ckd_date,
    pa.latest_ckd_date,
    pa.earliest_resolved_date,
    pa.latest_resolved_date,
    
    -- QOF-specific derived fields
    CASE 
        WHEN pa.latest_resolved_date IS NULL THEN FALSE
        WHEN pa.latest_ckd_date > pa.latest_resolved_date THEN FALSE
        ELSE TRUE
    END AS is_ckd_currently_resolved,
    
    -- CKD observation type determination
    CASE
        WHEN bo.is_ckd_diagnosis_code THEN 'CKD Diagnosis'
        WHEN bo.is_ckd_resolved_code THEN 'CKD Resolved'
        ELSE 'Unknown'
    END AS ckd_observation_type,
    
    -- QOF register eligibility context (basic - needs age filter and lab integration in fact layer)
    CASE
        WHEN pa.latest_ckd_date IS NOT NULL 
             AND (pa.latest_resolved_date IS NULL OR pa.latest_ckd_date > pa.latest_resolved_date)
        THEN TRUE
        ELSE FALSE
    END AS has_active_ckd_diagnosis,
    
    -- Clinical context flags
    CASE
        WHEN pa.latest_ckd_date IS NOT NULL AND pa.latest_ckd_date >= CURRENT_DATE - INTERVAL '12 months'
        THEN TRUE
        ELSE FALSE
    END AS has_recent_ckd_diagnosis,
    
    CASE
        WHEN pa.latest_ckd_date IS NOT NULL AND pa.latest_ckd_date >= CURRENT_DATE - INTERVAL '24 months'
        THEN TRUE
        ELSE FALSE
    END AS has_ckd_diagnosis_last_24m,
    
    -- Traceability arrays
    pa.all_ckd_concept_codes,
    pa.all_ckd_concept_displays,
    pa.all_resolved_concept_codes,
    pa.all_resolved_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 