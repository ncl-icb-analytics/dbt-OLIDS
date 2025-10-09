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
    src."lds_record_id" AS lds_record_id,
    src."id" AS id,
    src."practitioner_id" AS practitioner_id,
    src."organisation_id" AS organisation_id,
    src."role_code" AS role_code,
    src."role" AS role,
    src."date_employment_start" AS date_employment_start,
    src."date_employment_end" AS date_employment_end,
    src."lds_id" AS lds_id,
    src."lds_business_key" AS lds_business_key,
    src."lds_dataset_id" AS lds_dataset_id,
    src."lds_cdm_event_id" AS lds_cdm_event_id,
    src."lds_versioner_event_id" AS lds_versioner_event_id,
    src."record_owner_organisation_code" AS record_owner_organisation_code,
    src."lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    src."lds_initial_data_received_date" AS lds_initial_data_received_date,
    src."lds_is_deleted" AS lds_is_deleted,
    src."lds_start_date_time" AS lds_start_date_time,
    src."lds_lakehouse_date_processed" AS lds_lakehouse_date_processed,
    src."lds_lakehouse_datetime_updated" AS lds_lakehouse_datetime_updated
FROM {{ source('olids_common', 'PRACTITIONER_IN_ROLE') }} src
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code
