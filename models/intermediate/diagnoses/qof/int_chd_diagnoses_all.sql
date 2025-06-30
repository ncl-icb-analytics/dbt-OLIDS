{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All coronary heart disease (CHD) diagnoses from clinical records.
Uses QOF cluster ID CHD_COD for all forms of CHD diagnosis.

Clinical Purpose:
- CHD register inclusion for QOF cardiovascular disease management
- Cardiovascular risk stratification and monitoring
- Secondary prevention pathway identification

QOF Context:
CHD register follows simple diagnosis-only pattern - any CHD diagnosis code
qualifies for register inclusion. No resolution codes or complex criteria.
This is a lifelong condition register for cardiovascular secondary prevention.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per CHD observation.
Use this model as input for CHD register and cardiovascular risk models.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- CHD-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'CHD_COD' THEN TRUE ELSE FALSE END AS is_chd_diagnosis

FROM ({{ get_observations("'CHD_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
