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
    src."LakehouseDateProcessed",
    src."LakehouseDateTimeUpdated",
    src."lds_record_id",
    src."lds_id",
    src."id",
    src."lds_business_key",
    src."lds_datetime_data_acquired",
    src."lds_initial_data_received_date",
    src."lds_dataset_id",
    src."lds_start_date_time",
    src."organisation_code",
    src."assigning_authority_code",
    src."name",
    src."type_code",
    src."type_desc",
    src."postcode",
    src."parent_organisation_id",
    src."open_date",
    src."close_date",
    src."is_obsolete"
FROM {{ source('olids_core', 'ORGANISATION') }} src