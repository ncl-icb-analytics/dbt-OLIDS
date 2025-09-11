{{
    config(
        secure=true,
        alias='patient_registered_practitioner_in_role')
}}

/*
Base PATIENT_REGISTERED_PRACTITIONER_IN_ROLE View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
*/

SELECT
    src."LakehouseDateProcessed",
    src."LakehouseDateTimeUpdated",
    src."lds_record_id",
    src."lds_id",
    src."id",
    src."lds_business_key",
    src."lds_dataset_id",
    src."record_owner_organisation_code",
    src."lds_datetime_data_acquired",
    src."lds_start_date_time",
    src."person_id",
    src."patient_id",
    src."organisation_id",
    src."practitioner_id",
    src."episode_of_care_id",
    src."start_date",
    src."end_date"
FROM {{ source('olids_core', 'PATIENT_REGISTERED_PRACTITIONER_IN_ROLE') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients."id"
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code