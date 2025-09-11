{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All peripheral arterial disease (PAD) diagnoses from clinical records.
Uses QOF cluster ID PAD_COD for all forms of PAD diagnosis.

Clinical Purpose:
- PAD register inclusion for QOF cardiovascular disease management
- Cardiovascular risk stratification and monitoring
- Secondary prevention pathway identification

QOF Context:
PAD register follows simple diagnosis-only pattern - any PAD diagnosis code
qualifies for register inclusion. No resolution codes or complex criteria.
This is a lifelong condition register for cardiovascular secondary prevention.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per PAD observation.
Use this model as input for fct_person_pad_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- PAD-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'PAD_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,

    -- Observation type determination
    CASE
        WHEN obs.cluster_id = 'PAD_COD' THEN 'PAD Diagnosis'
        ELSE 'Unknown'
    END AS pad_observation_type

FROM ({{ get_observations("'PAD_COD'", source='PCD') }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
