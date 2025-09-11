{{
    config(
        secure=true,
        alias='flag')
}}

/*
Base FLAG View
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
    src."lds_initial_data_received_date",
    src."lds_start_date_time",
    src."person_id",
    src."patient_id",
    src."effective_date",
    src."expired_date",
    src."is_active",
    src."flag_text"
FROM {{ source('olids_core', 'FLAG') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients."id"
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code