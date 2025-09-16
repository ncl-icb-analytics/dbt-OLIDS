{{
    config(
        secure=true,
        alias='patient_person')
}}

/*
Base PATIENT_PERSON View
Generated from filtered patient data with deterministic person_id.
Pattern: Bridge table generated from patient base
*/

SELECT
    -- New Alpha columns first (following PATIENT_PERSON structure order)
    lds_lakehouse_date_processed AS lakehousedateprocessed,
    lds_lakehouse_datetime_updated AS lakehousedatetimeupdated,
    lds_record_id,
    -- Generate deterministic lds_id for patient_person bridge
    'pp-' || MD5(sk_patient_id) AS lds_id,
    -- Generate deterministic id for this bridge record
    'pp-' || MD5(sk_patient_id) AS id,
    lds_datetime_data_acquired,
    lds_start_date_time,
    lds_dataset_id,
    id AS patient_id,
    -- Generate deterministic person_id from sk_patient_id
    'ncl-person-' || MD5(sk_patient_id) AS person_id
FROM {{ ref('base_olids_patient') }} patients
WHERE sk_patient_id IS NOT NULL
    AND LENGTH(TRIM(sk_patient_id)) > 0  -- Ensure sk_patient_id is not empty after trimming
    AND is_dummy_patient = FALSE