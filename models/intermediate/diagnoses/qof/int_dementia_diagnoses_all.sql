{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All dementia diagnosis observations from clinical records.
Uses QOF dementia cluster ID:
- DEM_COD: Dementia diagnoses (no resolution codes as dementia is permanent)

Clinical Purpose:
- QOF dementia register data collection
- Dementia care pathway monitoring
- Cognitive assessment tracking
- Resolution status tracking

QOF Context:
Dementia register includes persons with dementia diagnosis codes.
Dementia is considered a permanent condition with no resolution codes.
No specific age restrictions for dementia register.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per dementia observation.
Use this model as input for fct_person_dementia_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Dementia-specific flags (observation-level only)
    TRUE AS is_dementia_diagnosis_code,

    -- Dementia observation type determination
    'Dementia Diagnosis' AS dementia_observation_type

FROM ({{ get_observations("'DEM_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
