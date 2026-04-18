{{
    config(
        secure=true,
        alias='flag')
}}

/*
Base FLAG View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Uses native person_id from source table.
*/

SELECT
    src.lds_record_id,
    src.id,
    {{ generate_person_id('src.person_id') }} AS person_id,
    src.patient_id,
    src.effective_date,
    src.expired_date,
    src.is_active,
    src.flag_text,
    src.lds_id,
    src.lds_business_key,
    src.lds_dataset_id,
    src.lds_cdm_event_id,
    src.lds_versioner_event_id,
    src.record_owner_organisation_code,
    src.lds_datetime_data_acquired,
    src.lds_initial_data_received_date,
    src.lds_is_deleted,
    src.lds_start_date_time,
    src.lds_lakehouse_date_processed,
    src.lds_lakehouse_datetime_updated
FROM {{ source('olids_common', 'FLAG') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('int_wnl_practices') }} wnl_practices
    ON src.record_owner_organisation_code = wnl_practices.practice_code
WHERE src.lds_start_date_time IS NOT NULL