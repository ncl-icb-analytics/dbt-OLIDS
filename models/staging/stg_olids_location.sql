-- Staging model for OLIDS_MASKED.LOCATION
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_initial_data_received_date" AS lds_initial_data_received_date,
    "lds_start_date_time" AS lds_start_date_time,
    "name" AS name,
    "type_code" AS type_code,
    "type_desc" AS type_desc,
    "is_primary_location" AS is_primary_location,
    "house_name" AS house_name,
    "house_number" AS house_number,
    "house_name_flat_number" AS house_name_flat_number,
    "street" AS street,
    "address_line_1" AS address_line_1,
    "address_line_2" AS address_line_2,
    "address_line_3" AS address_line_3,
    "address_line_4" AS address_line_4,
    "postcode" AS postcode,
    "managing_organisation_id" AS managing_organisation_id,
    "open_date" AS open_date,
    "close_date" AS close_date,
    "is_obsolete" AS is_obsolete
FROM {{ source('OLIDS_MASKED', 'LOCATION') }}
