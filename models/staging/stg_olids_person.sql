-- Staging model for OLIDS_MASKED.PERSON
-- Source: "Data_Store_OLIDS_UAT".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    -- TODO: lds_business_key column doesn't exist in PERSON
    -- Possible alternatives: unique_reference or id
    -- "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    -- TODO: primary_patient_id column doesn't exist in PERSON
    -- Possible alternatives: requesting_patient_id or id
    -- "primary_patient_id" AS primary_patient_id,
    "lds_start_date_time" AS lds_start_date_time,
    "lds_end_date_time" AS lds_end_date_time,
    -- Additional available columns for reference:
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "unique_reference" AS unique_reference,
    "requesting_patient_id" AS requesting_patient_id
FROM {{ source('OLIDS_MASKED', 'PERSON') }}
