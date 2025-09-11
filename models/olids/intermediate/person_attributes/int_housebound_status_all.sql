{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All housebound status observations from ECL source.
Uses cluster IDs: HOUSEBOUND and NO_LONGER_HOUSEBOUND.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS code_description,
    obs.cluster_id AS source_cluster_id
FROM ({{ get_observations("'HOUSEBOUND', 'NO_LONGER_HOUSEBOUND'", source='ECL_CACHE') }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
ORDER BY person_id, clinical_effective_date DESC