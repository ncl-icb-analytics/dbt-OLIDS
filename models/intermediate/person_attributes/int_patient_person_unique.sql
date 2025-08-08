{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}
/**
Deduplicated patient-person mappings.
Handles cases where multiple patient_ids may map to the same person_id.
When multiple patient_ids exist for the same person_id, selects the
lowest patient_id to ensure consistent mapping.
This intermediate model prevents duplicate persons in downstream models
that join observations/medications to person data.
EXCLUDES orphaned person records that don't link to valid patient records.
*/
SELECT
    pp.patient_id,
    pp.person_id
FROM {{ ref('stg_olids_patient_person') }} pp
INNER JOIN {{ ref('stg_olids_person') }} p
    ON pp.person_id = p.id
-- CRITICAL: Only include mappings where the patient actually exists
INNER JOIN {{ ref('stg_olids_patient') }} pat
    ON pp.patient_id = pat.id
WHERE pp.patient_id IS NOT NULL
    AND pp.person_id IS NOT NULL
    -- Only include patients with basic demographics
    AND pat.birth_year IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pp.person_id
    ORDER BY pp.patient_id
) = 1
ORDER BY person_id, patient_id