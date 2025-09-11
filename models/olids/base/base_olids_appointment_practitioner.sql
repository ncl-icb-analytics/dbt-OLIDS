{{
    config(
        secure=true,
        alias='appointment_practitioner')
}}

/*
Base APPOINTMENT_PRACTITIONER View  
Filters to NCL practices only.
Pattern: Infrastructure table with record_owner_organisation_code
*/

SELECT
    src."LakehouseDateProcessed",
    src."LakehouseDateTimeUpdated",
    src."lds_record_id",
    src."lds_id",
    src."id",
    src."lds_business_key",
    src."lds_dataset_id",
    src."lds_record_id_user",
    src."record_owner_organisation_code",
    src."lds_datetime_data_acquired",
    src."lds_initial_data_received_date",
    src."lds_start_date_time",
    src."appointment_id",
    src."practitioner_id",
    src."lds_end_date_time"
FROM {{ source('olids_core', 'APPOINTMENT_PRACTITIONER') }} src
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code