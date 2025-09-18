{{
    config(
        secure=true,
        alias='location')
}}

/*
Base LOCATION View
Reference data - no filtering applied.
Pattern: Global reference table
*/

SELECT
    src."lds_record_id" AS lds_record_id,
    src."id" AS id,
    src."name" AS name,
    src."type_code" AS type_code,
    src."type_desc" AS type_desc,
    src."is_primary_location" AS is_primary_location,
    src."house_name" AS house_name,
    src."house_number" AS house_number,
    src."house_name_flat_number" AS house_name_flat_number,
    src."street" AS street,
    src."address_line_1" AS address_line_1,
    src."address_line_2" AS address_line_2,
    src."address_line_3" AS address_line_3,
    src."address_line_4" AS address_line_4,
    src."postcode" AS postcode,
    src."managing_organisation_id" AS managing_organisation_id,
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
FROM {{ source('olids_core', 'LOCATION') }} src