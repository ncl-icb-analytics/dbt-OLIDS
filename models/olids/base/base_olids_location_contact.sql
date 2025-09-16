{{
    config(
        secure=true,
        alias='location_contact')
}}

/*
Base LOCATION_CONTACT View
Reference data - no filtering applied.
Pattern: Global reference table
*/

SELECT
    src."lds_record_id" AS lds_record_id,
    src."id" AS id,
    src."location_id" AS location_id,
    src."is_primary_contact" AS is_primary_contact,
    src."contact_type" AS contact_type,
    src."contact_type_concept_id" AS contact_type_concept_id,
    src."value" AS value,
    src."lds_id" AS lds_id,
    src."lds_business_key" AS lds_business_key,
    src."lds_dataset_id" AS lds_dataset_id,
    src."lds_cdm_event_id" AS lds_cdm_event_id,
    src."lds_versioner_event_id" AS lds_versioner_event_id,
    src."lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    src."lds_initial_data_received_date" AS lds_initial_data_received_date,
    src."lds_is_deleted" AS lds_is_deleted,
    src."lds_start_date_time" AS lds_start_date_time,
    src."lds_lakehouse_date_processed" AS lds_lakehouse_date_processed,
    src."lds_lakehouse_datetime_updated" AS lds_lakehouse_datetime_updated
FROM {{ source('olids_core', 'LOCATION_CONTACT') }} src