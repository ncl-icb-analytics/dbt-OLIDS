{{
    config(
        secure=true,
        alias='patient_address')
}}

/*
Base PATIENT_ADDRESS View
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
    src."lds_datetime_data_acquired",
    src."lds_start_date_time",
    src."record_owner_organisation_code",
    src."patient_id",
    src."address_type_concept_id",
    src."postcode_hash",
    src."start_date",
    src."end_date",
    src."lds_end_date_time",
    src."person_id"
FROM {{ source('olids_core', 'PATIENT_ADDRESS') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients."id"
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code