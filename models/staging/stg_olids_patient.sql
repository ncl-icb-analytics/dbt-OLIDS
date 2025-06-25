-- Staging model for OLIDS_MASKED.PATIENT
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_start_date_time" AS lds_start_date_time,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "nhs_number_hash" AS nhs_number_hash,
    "sk_patient_id" AS sk_patient_id,
    "title" AS title,
    "gender_concept_id" AS gender_concept_id,
    "registered_practice_id" AS registered_practice_id,
    "birth_year" AS birth_year,
    "birth_month" AS birth_month,
    "death_year" AS death_year,
    "death_month" AS death_month,
    "is_confidential" AS is_confidential,
    "is_dummy_patient" AS is_dummy_patient,
    "is_spine_sensitive" AS is_spine_sensitive,
    "lds_end_date_time" AS lds_end_date_time
FROM {{ source('OLIDS_MASKED', 'PATIENT') }}
