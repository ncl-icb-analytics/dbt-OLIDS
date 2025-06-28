-- Staging model for OLIDS_MASKED.ORGANISATION
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_initial_data_received_date" AS lds_initial_data_received_date,
    "lds_dataset_id" AS lds_dataset_id,
    "lds_start_date_time" AS lds_start_date_time,
    "organisation_code" AS organisation_code,
    "assigning_authority_code" AS assigning_authority_code,
    "name" AS name,
    "type_code" AS type_code,
    "type_desc" AS type_desc,
    "postcode" AS postcode,
    "parent_organisation_id" AS parent_organisation_id,
    "open_date" AS open_date,
    "close_date" AS close_date,
    "is_obsolete" AS is_obsolete
FROM {{ source('OLIDS_MASKED', 'ORGANISATION') }}
