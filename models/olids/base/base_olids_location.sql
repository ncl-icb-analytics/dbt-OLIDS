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
    src."name",
    src."type_code",
    src."type_desc",
    src."is_primary_location",
    src."house_name",
    src."house_number",
    src."house_name_flat_number",
    src."street",
    src."address_line_1",
    src."address_line_2",
    src."address_line_3",
    src."address_line_4",
    src."postcode",
    src."managing_organisation_id",
    src."open_date",
    src."close_date",
    src."is_obsolete"
FROM {{ source('olids_core', 'LOCATION') }} src