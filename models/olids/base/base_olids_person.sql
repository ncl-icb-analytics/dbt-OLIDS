{{
    config(
        secure=true,
        alias='person')
}}

/*
Base PERSON View
Sources native OLIDS_MASKED.PERSON, filtered to persons linked to NCL patients
via the PATIENT_PERSON bridge.
Pattern: id = numeric hash of native UUID (matches person_id on all other base
tables); person_uuid = native UUID.

Gender backfill: native PERSON.gender is currently 100% null upstream, so we
fall back to the gender_concept_id from the person's most recently registered
PATIENT row, resolved via OLIDS_TERMINOLOGY.CONCEPT.display. When native gender
is populated upstream it takes precedence via COALESCE.
*/

WITH gender_fallback AS (
    SELECT
        pp.person_uuid,
        c.display AS gender
    FROM {{ ref('base_olids_patient_person') }} pp
    INNER JOIN {{ ref('base_olids_patient') }} pat
        ON pp.patient_id = pat.id
    LEFT JOIN {{ ref('base_olids_concept') }} c
        ON pat.gender_concept_id = c.id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pp.person_uuid
        ORDER BY pat.lds_start_date_time DESC NULLS LAST
    ) = 1
)

SELECT
    {{ generate_person_id('per.id') }} AS id,
    per.id AS person_uuid,
    per.composite_id,
    per.matched_nhs_no_hash,
    COALESCE(per.gender, gf.gender) AS gender,
    per.birth_year,
    per.birth_month,
    per.death_year,
    per.death_month,
    per.death_notification_status,
    per.postcode_hash,
    per.preferred_contact_method,
    per.nominated_pharmacy,
    per.dispensing_doctor,
    per.medical_appliance_supplier,
    per.gp_practice_code,
    per.gp_registration_date,
    per.as_at_date,
    per.sensitivity_flag,
    per.error_success_code,
    per.lds_record_id,
    per.lds_id,
    per.lds_business_key,
    per.lds_dataset_id,
    per.lds_cdm_event_id,
    per.lds_datetime_data_acquired,
    per.lds_initial_data_received_date,
    per.lds_is_deleted,
    per.lds_start_date_time,
    per.lds_lakehouse_date_processed,
    per.lds_lakehouse_datetime_updated
FROM {{ source('olids_masked', 'PERSON') }} per
LEFT JOIN gender_fallback gf
    ON gf.person_uuid = per.id
WHERE EXISTS (
    SELECT 1
    FROM {{ ref('base_olids_patient_person') }} pp
    WHERE pp.person_uuid = per.id
)
