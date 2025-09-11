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
    src."LakehouseDateProcessed",
    src."LakehouseDateTimeUpdated",
    src."lds_record_id",
    src."lds_id",
    src."id",
    src."lds_business_key",
    src."lds_dataset_id",
    src."lds_datetime_data_acquired",
    src."lds_initial_data_received_date",
    src."lds_start_date_time",
    src."location_id",
    src."is_primary_contact",
    src."contact_type",
    src."contact_type_concept_id",
    src."value",
    src."lds_end_date_time"
FROM {{ source('olids_core', 'LOCATION_CONTACT') }} src