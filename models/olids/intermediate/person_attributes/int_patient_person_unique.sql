{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}
/**
Deduplicated patient-person mappings.
Now simplified since staging models handle UUID generation and deduplication.
The staging model stg_olids_patient_person generates consistent person_id values
from the PATIENT table via sk_patient_id hash.
EXCLUDES orphaned person records that don't link to valid patient records.
*/
SELECT DISTINCT
    pp.patient_id,
    pp.person_id,
    pat.sk_patient_id
FROM {{ ref('stg_olids_patient_person') }} pp
INNER JOIN {{ ref('stg_olids_person') }} p
    ON pp.person_id = p.id
-- Additional validation: ensure patient has basic demographics
INNER JOIN {{ ref('stg_olids_patient') }} pat
    ON pp.patient_id = pat.id
WHERE pp.patient_id IS NOT NULL
    AND pp.person_id IS NOT NULL
    AND pat.sk_patient_id IS NOT NULL
    -- Only include patients with basic demographics
    AND pat.birth_year IS NOT NULL
ORDER BY person_id, patient_id