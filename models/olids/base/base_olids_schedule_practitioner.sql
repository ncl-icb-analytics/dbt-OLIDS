{{
    config(
        secure=true,
        alias='schedule_practitioner')
}}

/*
Base SCHEDULE_PRACTITIONER View  
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
    src."record_owner_organisation_code",
    src."lds_datetime_data_acquired",
    src."lds_start_date_time",
    src."schedule_id",
    src."practitioner_id"
FROM {{ source('olids_core', 'SCHEDULE_PRACTITIONER') }} src
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code