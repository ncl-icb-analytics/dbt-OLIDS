{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All epilepsy diagnosis observations from clinical records.
Uses QOF epilepsy cluster IDs:
- EPI_COD: Epilepsy diagnoses
- EPIRES_COD: Epilepsy resolved/remission codes

Clinical Purpose:
- QOF epilepsy register data collection
- Epilepsy care pathway monitoring
- Seizure management tracking
- Resolution status tracking

QOF Context:
Epilepsy register includes persons with epilepsy diagnosis codes who have not
been resolved. Resolution logic applied in downstream fact models.
Age restrictions typically ≥18 years applied in fact layer.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per epilepsy observation.
Use this model as input for fct_person_epilepsy_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Epilepsy-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'EPI_COD' THEN TRUE ELSE FALSE END AS is_epilepsy_diagnosis_code,
    CASE WHEN obs.cluster_id = 'EPIRES_COD' THEN TRUE ELSE FALSE END AS is_epilepsy_resolved_code,

    -- Epilepsy observation type determination
    CASE
        WHEN obs.cluster_id = 'EPI_COD' THEN 'Epilepsy Diagnosis'
        WHEN obs.cluster_id = 'EPIRES_COD' THEN 'Epilepsy Resolved'
        ELSE 'Unknown'
    END AS epilepsy_observation_type

FROM ({{ get_observations("'EPI_COD', 'EPIRES_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
