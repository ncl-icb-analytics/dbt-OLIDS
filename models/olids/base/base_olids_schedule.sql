{{
    config(
        secure=true,
        alias='schedule')
}}

/*
Base SCHEDULE View  
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
    src."lds_datetime_data_acquired",
    src."lds_start_date_time",
    src."record_owner_organisation_code",
    src."location_id",
    src."location",
    src."practitioner_id",
    src."start_date",
    src."end_date",
    src."type",
    src."name",
    src."is_private"
FROM {{ source('olids_core', 'SCHEDULE') }} src
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code