{{
    config(
        secure=true,
        alias='practitioner_in_role')
}}

/*
Base PRACTITIONER_IN_ROLE View  
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
    src."practitioner_id",
    src."organisation_id",
    src."role_code",
    src."role",
    src."date_employment_start",
    src."date_employment_end"
FROM {{ source('olids_core', 'PRACTITIONER_IN_ROLE') }} src
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code