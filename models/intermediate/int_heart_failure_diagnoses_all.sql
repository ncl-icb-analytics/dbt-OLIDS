{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All heart failure diagnosis observations from clinical records.
Uses QOF heart failure cluster IDs:
- HF_COD: Heart failure diagnoses
- HFRES_COD: Heart failure resolved/remission codes
- HFLVSD_COD: Heart failure with left ventricular systolic dysfunction  
- REDEJCFRAC_COD: Reduced ejection fraction diagnoses

Clinical Purpose:
- QOF heart failure register data collection
- Heart failure type classification (HFrEF vs HFpEF identification)
- Cardiac function monitoring
- Resolution status tracking

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_heart_failure_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Flag different types of heart failure codes following QOF definitions
        CASE WHEN obs.cluster_id = 'HF_COD' THEN TRUE ELSE FALSE END AS is_heart_failure_diagnosis_code,
        CASE WHEN obs.cluster_id = 'HFRES_COD' THEN TRUE ELSE FALSE END AS is_heart_failure_resolved_code,
        CASE WHEN obs.cluster_id = 'HFLVSD_COD' THEN TRUE ELSE FALSE END AS is_hf_lvsd_code,
        CASE WHEN obs.cluster_id = 'REDEJCFRAC_COD' THEN TRUE ELSE FALSE END AS is_reduced_ef_code
        
    FROM ({{ get_observations("'HF_COD', 'HFRES_COD', 'HFLVSD_COD', 'REDEJCFRAC_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    -- Calculate person-level heart failure date aggregates for context
    SELECT
        person_id,
        
        -- Heart failure diagnosis dates
        MIN(CASE WHEN is_heart_failure_diagnosis_code THEN clinical_effective_date END) AS earliest_hf_date,
        MAX(CASE WHEN is_heart_failure_diagnosis_code THEN clinical_effective_date END) AS latest_hf_date,
        
        -- Specific HF type dates
        MIN(CASE WHEN is_hf_lvsd_code THEN clinical_effective_date END) AS earliest_hf_lvsd_date,
        MAX(CASE WHEN is_hf_lvsd_code THEN clinical_effective_date END) AS latest_hf_lvsd_date,
        MIN(CASE WHEN is_reduced_ef_code THEN clinical_effective_date END) AS earliest_reduced_ef_date,
        MAX(CASE WHEN is_reduced_ef_code THEN clinical_effective_date END) AS latest_reduced_ef_date,
        
        -- Resolution dates
        MIN(CASE WHEN is_heart_failure_resolved_code THEN clinical_effective_date END) AS earliest_resolved_date,
        MAX(CASE WHEN is_heart_failure_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_heart_failure_diagnosis_code THEN concept_code ELSE NULL END) AS all_hf_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_heart_failure_diagnosis_code THEN concept_display ELSE NULL END) AS all_hf_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_hf_lvsd_code THEN concept_code ELSE NULL END) AS all_hf_lvsd_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_reduced_ef_code THEN concept_code ELSE NULL END) AS all_reduced_ef_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_heart_failure_resolved_code THEN concept_code ELSE NULL END) AS all_resolved_concept_codes
            
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
    
    -- Heart failure type flags
    bo.is_heart_failure_diagnosis_code,
    bo.is_heart_failure_resolved_code,
    bo.is_hf_lvsd_code,
    bo.is_reduced_ef_code,
    
    -- Person-level aggregate context (for downstream QOF logic)
    pa.earliest_hf_date,
    pa.latest_hf_date,
    pa.earliest_hf_lvsd_date,
    pa.latest_hf_lvsd_date,
    pa.earliest_reduced_ef_date,
    pa.latest_reduced_ef_date,
    pa.earliest_resolved_date,
    pa.latest_resolved_date,
    
    -- QOF-specific derived fields
    CASE 
        WHEN pa.latest_resolved_date IS NULL THEN FALSE
        WHEN pa.latest_hf_date > pa.latest_resolved_date THEN FALSE
        ELSE TRUE
    END AS is_heart_failure_currently_resolved,
    
    -- Heart failure observation type determination
    CASE
        WHEN bo.is_heart_failure_diagnosis_code THEN 'Heart Failure Diagnosis'
        WHEN bo.is_hf_lvsd_code THEN 'HF with LVSD'
        WHEN bo.is_reduced_ef_code THEN 'Reduced Ejection Fraction'
        WHEN bo.is_heart_failure_resolved_code THEN 'Heart Failure Resolved'
        ELSE 'Unknown'
    END AS heart_failure_observation_type,
    
    -- QOF register eligibility context (basic)
    CASE
        WHEN pa.latest_hf_date IS NOT NULL 
             AND (pa.latest_resolved_date IS NULL OR pa.latest_hf_date > pa.latest_resolved_date)
        THEN TRUE
        ELSE FALSE
    END AS has_active_heart_failure_diagnosis,
    
    -- Heart failure type indicators (for clinical classification)
    CASE
        WHEN pa.latest_hf_lvsd_date IS NOT NULL OR pa.latest_reduced_ef_date IS NOT NULL
        THEN TRUE
        ELSE FALSE
    END AS has_heart_failure_reduced_ef_indicators,
    
    -- Most recent HF type classification
    CASE
        WHEN pa.latest_hf_lvsd_date IS NOT NULL OR pa.latest_reduced_ef_date IS NOT NULL
        THEN 'HFrEF (Heart Failure with Reduced Ejection Fraction)'
        WHEN pa.latest_hf_date IS NOT NULL
        THEN 'Heart Failure (Type Unspecified)'
        ELSE NULL
    END AS inferred_heart_failure_type,
    
    -- Traceability arrays
    pa.all_hf_concept_codes,
    pa.all_hf_concept_displays,
    pa.all_hf_lvsd_concept_codes,
    pa.all_reduced_ef_concept_codes,
    pa.all_resolved_concept_codes

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 