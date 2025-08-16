{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All frailty diagnosis observations from clinical records.
Uses frailty cluster ID:
- FRAILTY_DX: Frailty diagnoses (includes mild, moderate, severe frailty states)

Clinical Purpose:
- Frailty register data collection
- Frailty assessment and stratification
- Care planning for frail elderly populations
- Risk assessment and intervention tracking

Clinical Context:
Frailty register includes persons with frailty diagnosis codes.
Frailty can range from mild to severe and may change over time.
No specific resolution codes as frailty status can fluctuate.

Frailty codes included:
- 248279007: Frailty
- 404904002: Frail elderly
- 925791000000100: Mild frailty
- 925831000000107: Moderate frailty
- 925861000000102: Severe frailty

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per frailty observation.
Use this model as input for fct_person_frailty_register.sql which applies business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Frailty-specific flags (observation-level only)
    TRUE AS is_diagnosis_code,

    -- Frailty severity determination based on concept codes
    CASE
        WHEN obs.mapped_concept_code = '925791000000100' THEN 'Mild'
        WHEN obs.mapped_concept_code = '925831000000107' THEN 'Moderate'
        WHEN obs.mapped_concept_code = '925861000000102' THEN 'Severe'
        ELSE 'Unknown'
    END AS frailty_severity,

    -- Frailty observation type
    'Frailty Diagnosis' AS frailty_observation_type

FROM ({{ get_observations("'FRAILTY_DX'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id