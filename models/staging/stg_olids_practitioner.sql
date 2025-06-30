-- Staging model for OLIDS_MASKED.PRACTITIONER
-- Source: "Data_Store_OLIDS_UAT".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_start_date_time" AS lds_start_date_time,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "gmc_code" AS gmc_code,
    "title" AS title,
    "first_name" AS first_name,
    "last_name" AS last_name,
    "name" AS name,
    "is_obsolete" AS is_obsolete,
    "lds_end_date_time" AS lds_end_date_time
FROM {{ source('OLIDS_MASKED', 'PRACTITIONER') }}
