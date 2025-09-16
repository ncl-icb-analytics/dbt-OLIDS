{{
    config(
        secure=true,
        alias='person')
}}

/*
Base PERSON View
Generated person dimension from filtered patients.
Pattern: Dimension generated from patient base  
*/

SELECT DISTINCT
    id,
    nhs_number_hash,
    title,
    gender_concept_id,
    birth_year,
    birth_month,
    death_year,
    death_month
FROM {{ ref('base_olids_patient') }}