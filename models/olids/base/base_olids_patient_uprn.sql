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
    src.lds_record_id,
    src.id,
    src.registrar_event_id,
    src.masked_uprn,
    src.masked_usrn,
    src.masked_postcode,
    src.address_format_quality,
    src.post_code_quality,
    src.matched_with_assign,
    src.qualifier,
    src.uprn_property_classification,
    src.algorithm,
    src.match_pattern,
    src.lds_id,
    src.lds_business_key,
    src.lds_dataset_id,
    src.lds_cdm_event_id,
    src.lds_registrar_event_id,
    src.record_owner_organisation_code,
    src.lds_datetime_data_acquired,
    src.lds_initial_data_received_date,
    src.lds_is_deleted,
    src.lds_start_date_time,
    src.lds_lakehouse_date_processed,
    src.lds_lakehouse_datetime_updated
FROM {{ source('olids_masked', 'PATIENT_UPRN') }} src
