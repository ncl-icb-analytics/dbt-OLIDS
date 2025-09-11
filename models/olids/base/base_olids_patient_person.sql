{{
    config(
        secure=true,
        alias='patient_person')
}}

/*
Base PATIENT_PERSON View
Generated from filtered patient data.
Pattern: Bridge table generated from patient base
*/

SELECT 
    patients."id" AS patient_id,
    patients."id" AS person_id  -- Generated relationship per issue #192
FROM {{ ref('base_olids_patient') }} patients