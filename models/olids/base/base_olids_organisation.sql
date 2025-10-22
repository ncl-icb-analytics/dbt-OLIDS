{{
    config(
        secure=true,
        alias='organisation')
}}

/*
Base ORGANISATION View
Reference data - no filtering applied.
Pattern: Global reference table
*/

SELECT
    src.lds_record_id,
    src.id,
    src.organisation_code,
    src.assigning_authority_code,
    src.name,
    src.type_code,
    src.type_desc,
    src.postcode,
    src.parent_organisation_id,
    src.open_date,
    src.close_date,
    src.is_obsolete,
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
FROM {{ source('olids_common', 'ORGANISATION') }} src
WHERE src.organisation_code IS NOT NULL
    AND src.lds_start_date_time IS NOT NULL