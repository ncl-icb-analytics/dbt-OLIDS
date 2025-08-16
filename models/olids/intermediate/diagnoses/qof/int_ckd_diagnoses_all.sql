{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All chronic kidney disease (CKD) diagnosis observations from clinical records.
Uses QOF CKD cluster IDs:
- CKD_COD: CKD diagnoses
- CKDRES_COD: CKD resolved/remission codes

Clinical Purpose:
- QOF CKD register data collection
- Kidney function monitoring
- CKD staging and progression tracking
- Resolution status monitoring

QOF Context:
CKD register includes persons with CKD diagnosis codes who have not
been resolved. Resolution logic applied in downstream fact models.
Age restrictions typically ≥18 years applied in fact layer.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per CKD observation.
Use this model as input for fct_person_ckd_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- CKD-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'CKD_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,
    CASE WHEN obs.cluster_id = 'CKDRES_COD' THEN TRUE ELSE FALSE END AS is_resolved_code,

    -- CKD observation type determination
    CASE
        WHEN obs.cluster_id = 'CKD_COD' THEN 'CKD Diagnosis'
        WHEN obs.cluster_id = 'CKDRES_COD' THEN 'CKD Resolved'
        ELSE 'Unknown'
    END AS ckd_observation_type

FROM ({{ get_observations("'CKD_COD', 'CKDRES_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
