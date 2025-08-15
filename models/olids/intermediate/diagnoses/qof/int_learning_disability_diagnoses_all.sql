{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All learning disability diagnosis observations from clinical records.
Uses QOF learning disability cluster IDs:
- LD_COD: Learning disability diagnoses

Clinical Purpose:
- QOF learning disability register data collection
- Learning disability support and monitoring
- Annual health checks eligibility
- Resolution status tracking

QOF Context:
Learning disability register includes persons with learning disability diagnosis codes
who have not been resolved. Resolution logic applied in downstream fact models.
Age restrictions typically ≥14 years applied in fact layer.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per learning disability observation.
Use this model as input for fct_person_learning_disability_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Learning disability-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'LD_COD' THEN TRUE ELSE FALSE END AS is_learning_disability_diagnosis_code,

    -- Learning disability observation type determination
    CASE
        WHEN obs.cluster_id = 'LD_COD' THEN 'Learning Disability Diagnosis'
        ELSE 'Unknown'
    END AS learning_disability_observation_type

FROM ({{ get_observations("'LD_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
