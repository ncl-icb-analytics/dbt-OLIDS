-- Staging model for OLIDS_MASKED.PERSON_BACKUP
-- Source: "Data_Store_OLIDS_UAT".OLIDS_MASKED
-- Using PERSON_BACKUP as PERSON table was deleted

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "primary_patient_id" AS primary_patient_id,
    "lds_start_date_time" AS lds_start_date_time,
    "lds_end_date_time" AS lds_end_date_time
FROM {{ source('OLIDS_MASKED', 'PERSON_BACKUP') }}
