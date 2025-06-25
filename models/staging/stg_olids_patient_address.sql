-- Staging model for OLIDS_MASKED.PATIENT_ADDRESS
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_start_date_time" AS lds_start_date_time,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "patient_id" AS patient_id,
    "address_type_concept_id" AS address_type_concept_id,
    "post_code_hash" AS post_code_hash,
    "start_date" AS start_date,
    "end_date" AS end_date,
    "lds_end_date_time" AS lds_end_date_time
FROM {{ source('OLIDS_MASKED', 'PATIENT_ADDRESS') }}
