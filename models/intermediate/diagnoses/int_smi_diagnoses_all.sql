{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All serious mental illness (SMI) diagnosis observations from clinical records.
Uses QOF SMI cluster IDs:
- SMI_COD: SMI diagnoses (schizophrenia, bipolar disorder, other psychoses)
- SMIRES_COD: SMI resolved/remission codes

Clinical Purpose:
- QOF SMI register data collection
- Mental health care pathway monitoring
- SMI treatment tracking
- Resolution status tracking

QOF Context:
SMI register includes persons with SMI diagnosis codes who have not
been resolved. Resolution logic applied in downstream fact models.
Age restrictions typically ≥18 years applied in fact layer.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per SMI observation.
Use this model as input for fct_person_smi_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    
    -- SMI-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'SMI_COD' THEN TRUE ELSE FALSE END AS is_smi_diagnosis_code,
    CASE WHEN obs.cluster_id = 'SMIRES_COD' THEN TRUE ELSE FALSE END AS is_smi_resolved_code,
    
    -- SMI observation type determination
    CASE
        WHEN obs.cluster_id = 'SMI_COD' THEN 'SMI Diagnosis'
        WHEN obs.cluster_id = 'SMIRES_COD' THEN 'SMI Resolved'
        ELSE 'Unknown'
    END AS smi_observation_type

FROM ({{ get_observations("'SMI_COD', 'SMIRES_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id 