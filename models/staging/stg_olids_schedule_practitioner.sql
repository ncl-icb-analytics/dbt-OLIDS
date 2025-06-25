-- Staging model for OLIDS_MASKED.SCHEDULE_PRACTITIONER
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_start_date_time" AS lds_start_date_time,
    "schedule_id" AS schedule_id,
    "practitioner_id" AS practitioner_id
FROM {{ source('OLIDS_MASKED', 'SCHEDULE_PRACTITIONER') }}
