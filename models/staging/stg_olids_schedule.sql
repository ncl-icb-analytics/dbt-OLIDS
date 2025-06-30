-- Staging model for OLIDS_MASKED.SCHEDULE
-- Source: "Data_Store_OLIDS_UAT".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_start_date_time" AS lds_start_date_time,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "location_id" AS location_id,
    "location" AS location,
    "practitioner_id" AS practitioner_id,
    "start_date" AS start_date,
    "end_date" AS end_date,
    "type" AS type,
    "name" AS name,
    "is_private" AS is_private
FROM {{ source('OLIDS_MASKED', 'SCHEDULE') }}
