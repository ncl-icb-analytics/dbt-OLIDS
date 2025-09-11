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
    src."LakehouseDateProcessed",
    src."LakehouseDateTimeUpdated",
    src."lds_record_id",
    src."lds_id",
    src."id",
    src."lds_dataset_id",
    src."lds_datetime_data_acquired",
    src."lds_start_date_time",
    src."registrar_event_id",
    src."masked_uprn",
    src."masked_usrn",
    src."masked_postcode",
    src."address_format_quality",
    src."post_code_quality",
    src."matched_with_assign",
    src."qualifier",
    src."uprn_property_classification",
    src."algorithm",
    src."match_pattern",
    src."lds_end_date_time"
FROM {{ source('olids_core', 'PATIENT_UPRN') }} src