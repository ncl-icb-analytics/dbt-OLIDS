{{
    config(
        secure=true,
        alias='person')
}}

/*
Base PERSON View
Generated person dimension from filtered patients with deterministic person_id.
Pattern: Dimension generated from patient base
*/

SELECT DISTINCT
    -- Generate deterministic person_id matching patient_person bridge
    'ncl-person-' || MD5(sk_patient_id) AS id,
    nhs_number_hash,
    title,
    gender_concept_id,
    birth_year,
    birth_month,
    death_year,
    death_month
FROM {{ ref('base_olids_patient') }}
WHERE sk_patient_id IS NOT NULL
    AND LENGTH(TRIM(sk_patient_id)) > 0