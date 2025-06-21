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
This is OBSERVATION-LEVEL data - one row per heart failure observation.
Use this model as input for fct_person_heart_failure_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    
    -- Heart failure-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'HF_COD' THEN TRUE ELSE FALSE END AS is_heart_failure_diagnosis_code,
    CASE WHEN obs.cluster_id = 'HFRES_COD' THEN TRUE ELSE FALSE END AS is_heart_failure_resolved_code,
    CASE WHEN obs.cluster_id = 'HFLVSD_COD' THEN TRUE ELSE FALSE END AS is_hf_lvsd_code,
    CASE WHEN obs.cluster_id = 'REDEJCFRAC_COD' THEN TRUE ELSE FALSE END AS is_reduced_ef_code,
    
    -- Heart failure observation type determination
    CASE
        WHEN obs.cluster_id = 'HF_COD' THEN 'Heart Failure Diagnosis'
        WHEN obs.cluster_id = 'HFLVSD_COD' THEN 'HF with LVSD'
        WHEN obs.cluster_id = 'REDEJCFRAC_COD' THEN 'Reduced Ejection Fraction'
        WHEN obs.cluster_id = 'HFRES_COD' THEN 'Heart Failure Resolved'
        ELSE 'Unknown'
    END AS heart_failure_observation_type

FROM ({{ get_observations("'HF_COD', 'HFRES_COD', 'HFLVSD_COD', 'REDEJCFRAC_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id 