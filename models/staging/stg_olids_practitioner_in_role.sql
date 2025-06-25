-- Staging model for OLIDS_MASKED.PRACTITIONER_IN_ROLE
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_start_date_time" AS lds_start_date_time,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "practitioner_id" AS practitioner_id,
    "organisation_id" AS organisation_id,
    "role_code" AS role_code,
    "role" AS role,
    "date_employment_start" AS date_employment_start,
    "date_employment_end" AS date_employment_end
FROM {{ source('OLIDS_MASKED', 'PRACTITIONER_IN_ROLE') }}
