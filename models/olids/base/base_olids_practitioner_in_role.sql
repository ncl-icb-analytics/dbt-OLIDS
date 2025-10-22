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
    src.lds_record_id,
    src.id,
    src.practitioner_id,
    src.organisation_id,
    src.role_code,
    src.role,
    src.date_employment_start,
    src.date_employment_end,
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
FROM {{ source('olids_common', 'PRACTITIONER_IN_ROLE') }} src
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src.record_owner_organisation_code = ncl_practices.practice_code
