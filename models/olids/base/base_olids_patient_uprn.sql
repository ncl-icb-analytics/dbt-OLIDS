{{
    config(
        secure=true,
        alias='patient_uprn')
}}

/*
Base PATIENT_UPRN View
Reference data - no filtering applied.
Pattern: Global reference table
*/

SELECT
    src."lds_record_id" AS lds_record_id,
    src."id" AS id,
    src."registrar_event_id" AS registrar_event_id,
    src."masked_uprn" AS masked_uprn,
    src."masked_usrn" AS masked_usrn,
    src."masked_postcode" AS masked_postcode,
    src."address_format_quality" AS address_format_quality,
    src."post_code_quality" AS post_code_quality,
    src."matched_with_assign" AS matched_with_assign,
    src."qualifier" AS qualifier,
    src."uprn_property_classification" AS uprn_property_classification,
    src."algorithm" AS algorithm,
    src."match_pattern" AS match_pattern,
    src."lds_id" AS lds_id,
    src."lds_business_key" AS lds_business_key,
    src."lds_dataset_id" AS lds_dataset_id,
    src."lds_cdm_event_id" AS lds_cdm_event_id,
    src."lds_registrar_event_id" AS lds_registrar_event_id,
    src."record_owner_organisation_code" AS record_owner_organisation_code,
    src."lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    src."lds_initial_data_received_date" AS lds_initial_data_received_date,
    src."lds_is_deleted" AS lds_is_deleted,
    src."lds_start_date_time" AS lds_start_date_time,
    src."lds_lakehouse_date_processed" AS lds_lakehouse_date_processed,
    src."lds_lakehouse_datetime_updated" AS lds_lakehouse_datetime_updated
FROM {{ source('olids_masked', 'PATIENT_UPRN') }} src
