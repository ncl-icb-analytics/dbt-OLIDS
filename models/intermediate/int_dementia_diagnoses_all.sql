{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All dementia diagnosis observations from clinical records.
Uses QOF dementia cluster IDs:
- DEM_COD: Dementia diagnoses
- DEMRES_COD: Dementia resolved/remission codes

Clinical Purpose:
- QOF dementia register data collection
- Dementia care pathway monitoring
- Cognitive assessment tracking
- Resolution status tracking

QOF Context:
Dementia register includes persons with dementia diagnosis codes who have not
been resolved. Resolution logic applied in downstream fact models.
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
    CASE WHEN obs.cluster_id = 'DEM_COD' THEN TRUE ELSE FALSE END AS is_dementia_diagnosis_code,
    CASE WHEN obs.cluster_id = 'DEMRES_COD' THEN TRUE ELSE FALSE END AS is_dementia_resolved_code,
    
    -- Dementia observation type determination
    CASE
        WHEN obs.cluster_id = 'DEM_COD' THEN 'Dementia Diagnosis'
        WHEN obs.cluster_id = 'DEMRES_COD' THEN 'Dementia Resolved'
        ELSE 'Unknown'
    END AS dementia_observation_type

FROM ({{ get_observations("'DEM_COD', 'DEMRES_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id 