{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All hypertension diagnosis observations from clinical records.
Uses QOF hypertension cluster IDs:
- HYP_COD: Hypertension diagnoses
- HYPRES_COD: Hypertension resolved/remission codes

Clinical Purpose:
- QOF hypertension register data collection
- Blood pressure management monitoring
- Cardiovascular risk assessment support
- Resolution status tracking

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per hypertension observation.
Use this model as input for fct_person_hypertension_register.sql which applies person-level aggregation and QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Flag different types of hypertension codes following QOF definitions
    CASE WHEN obs.cluster_id = 'HYP_COD' THEN TRUE ELSE FALSE END AS is_hypertension_diagnosis_code,
    CASE WHEN obs.cluster_id = 'HYPRES_COD' THEN TRUE ELSE FALSE END AS is_hypertension_resolved_code,

    -- Hypertension observation type determination
    CASE
        WHEN obs.cluster_id = 'HYP_COD' THEN 'Hypertension Diagnosis'
        WHEN obs.cluster_id = 'HYPRES_COD' THEN 'Hypertension Resolved'
        ELSE 'Unknown'
    END AS hypertension_observation_type

FROM ({{ get_observations("'HYP_COD', 'HYPRES_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
ORDER BY person_id, clinical_effective_date, observation_id
