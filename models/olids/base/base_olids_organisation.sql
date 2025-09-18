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
    src."lds_record_id" AS lds_record_id,
    src."id" AS id,
    src."organisation_code" AS organisation_code,
    src."assigning_authority_code" AS assigning_authority_code,
    src."name" AS name,
    src."type_code" AS type_code,
    src."type_desc" AS type_desc,
    src."postcode" AS postcode,
    src."parent_organisation_id" AS parent_organisation_id,
    src."open_date" AS open_date,
    src."close_date" AS close_date,
    src."is_obsolete" AS is_obsolete,
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
FROM {{ source('olids_core', 'ORGANISATION') }} src