{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'sex'],
        cluster_by=['person_id'])
}}

-- Person Sex Dimension Table
-- Derives sex from gender concepts using dynamic concept lookups

SELECT DISTINCT
    pp.person_id,
    COALESCE(target_concept.display, source_concept.display, 'Unknown') AS sex
FROM {{ ref('stg_olids_patient') }} AS p
INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
    ON p.id = pp.patient_id
{{ join_concept_display('p.gender_concept_id') }}
