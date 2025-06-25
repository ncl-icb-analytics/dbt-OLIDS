{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All depression diagnosis observations from clinical records.
Uses QOF depression cluster IDs:
- DEPR_COD: Depression diagnoses
- DEPRES_COD: Depression resolved/remission codes

Clinical Purpose:
- QOF depression register data collection
- Mental health care pathway monitoring
- Depression severity tracking
- Resolution status tracking

QOF Context:
Depression register includes persons with depression diagnosis codes who have not
been resolved. Resolution logic applied in downstream fact models.
Age restrictions typically ≥18 years applied in fact layer.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per depression observation.
Use this model as input for fct_person_depression_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Depression-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'DEPR_COD' THEN TRUE ELSE FALSE END AS is_depression_diagnosis_code,
    CASE WHEN obs.cluster_id = 'DEPRES_COD' THEN TRUE ELSE FALSE END AS is_depression_resolved_code,

    -- Depression observation type determination
    CASE
        WHEN obs.cluster_id = 'DEPR_COD' THEN 'Depression Diagnosis'
        WHEN obs.cluster_id = 'DEPRES_COD' THEN 'Depression Resolved'
        ELSE 'Unknown'
    END AS depression_observation_type

FROM ({{ get_observations("'DEPR_COD', 'DEPRES_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
