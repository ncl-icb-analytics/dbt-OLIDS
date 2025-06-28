-- Staging model for OLIDS_MASKED.PATIENT_PERSON
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_start_date_time" AS lds_start_date_time,
    "lds_end_date_time" AS lds_end_date_time,
    "lds_dataset_id" AS lds_dataset_id,
    "patient_id" AS patient_id,
    "person_id" AS person_id
FROM {{ source('OLIDS_MASKED', 'PATIENT_PERSON') }}
