-- Staging model for OLIDS_MASKED.FLAG
-- Source: "Data_Store_OLIDS_UAT".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_initial_data_received_date" AS lds_initial_data_received_date,
    "lds_start_date_time" AS lds_start_date_time,
    "person_id" AS person_id,
    "patient_id" AS patient_id,
    "effective_date" AS effective_date,
    "expired_date" AS expired_date,
    "is_active" AS is_active,
    "flag_text" AS flag_text
FROM {{ source('OLIDS_MASKED', 'FLAG') }}
