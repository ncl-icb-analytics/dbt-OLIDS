{{
    config(
        secure=true,
        alias='practitioner')
}}

/*
Base PRACTITIONER View  
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
    src."gmc_code",
    src."title",
    src."first_name",
    src."last_name",
    src."name",
    src."is_obsolete",
    src."lds_end_date_time"
FROM {{ source('olids_core', 'PRACTITIONER') }} src
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code