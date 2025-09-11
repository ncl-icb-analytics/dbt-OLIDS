{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
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
This is OBSERVATION-LEVEL data - one row per COPD observation.
Use this model as input for fct_person_copd_register.sql which applies QOF business rules and spirometry requirements.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- COPD-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'COPD_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,
    CASE WHEN obs.cluster_id = 'COPDRES_COD' THEN TRUE ELSE FALSE END AS is_resolved_code,

    -- COPD observation type determination
    CASE
        WHEN obs.cluster_id = 'COPD_COD' THEN 'COPD Diagnosis'
        WHEN obs.cluster_id = 'COPDRES_COD' THEN 'COPD Resolved'
        ELSE 'Unknown'
    END AS copd_observation_type

FROM ({{ get_observations("'COPD_COD', 'COPDRES_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
