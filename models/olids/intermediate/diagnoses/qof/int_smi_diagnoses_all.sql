{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All serious mental illness (SMI) diagnosis observations from clinical records.
Uses QOF SMI cluster IDs:
- MH_COD: Mental health diagnoses (schizophrenia, bipolar disorder, other psychoses)
- MHREM_COD: Mental health remission codes

Clinical Purpose:
- QOF SMI register data collection
- Mental health care pathway monitoring
- SMI treatment tracking
- Resolution status tracking

QOF Context:
SMI register includes persons with mental health diagnosis codes who have not
been resolved (no recent remission codes). Resolution logic applied in downstream fact models.
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
    CASE WHEN obs.cluster_id = 'MH_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,
    CASE WHEN obs.cluster_id = 'MHREM_COD' THEN TRUE ELSE FALSE END AS is_resolved_code,

    -- SMI observation type determination
    CASE
        WHEN obs.cluster_id = 'MH_COD' THEN 'SMI Diagnosis'
        WHEN obs.cluster_id = 'MHREM_COD' THEN 'SMI Resolved'
        ELSE 'Unknown'
    END AS smi_observation_type

FROM ({{ get_observations("'MH_COD', 'MHREM_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
