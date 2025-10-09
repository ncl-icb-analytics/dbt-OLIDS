{{
    config(
        secure=true,
        alias='person')
}}

/*
Base PERSON View
Generated person dimension from filtered patients with fabricated person_id.
Pattern: Dimension generated from patient_person bridge
*/

SELECT DISTINCT
    pp.person_id AS id,
    p.nhs_number_hash,
    p.title,
    p.gender_concept_id,
    p.birth_year,
    p.birth_month,
    p.death_year,
    p.death_month
FROM {{ ref('base_olids_patient') }} p
INNER JOIN {{ ref('base_olids_patient_person') }} pp
    ON p.id = pp.patient_id