{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All cancer diagnosis observations from clinical records.
Uses QOF cancer cluster IDs:
- CAN_COD: Cancer diagnoses

Clinical Purpose:
- QOF cancer register data collection
- Cancer care pathway monitoring
- Oncology treatment tracking
- Resolution/remission status tracking

QOF Context:
Cancer register includes persons with cancer diagnosis codes who have not
been resolved/in remission. Resolution logic applied in downstream fact models.
No specific age restrictions for cancer register.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per cancer observation.
Use this model as input for fct_person_cancer_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Cancer-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'CAN_COD' THEN TRUE ELSE FALSE END AS is_cancer_diagnosis_code,

    -- Cancer observation type determination
    CASE
        WHEN obs.cluster_id = 'CAN_COD' THEN 'Cancer Diagnosis'
        ELSE 'Unknown'
    END AS cancer_observation_type

FROM ({{ get_observations("'CAN_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
