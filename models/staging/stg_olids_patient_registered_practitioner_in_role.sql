-- Staging model for OLIDS_MASKED.PATIENT_REGISTERED_PRACTITIONER_IN_ROLE
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_start_date_time" AS lds_start_date_time,
    "person_id" AS person_id,
    "patient_id" AS patient_id,
    "organisation_id" AS organisation_id,
    "practitioner_id" AS practitioner_id,
    "episode_of_care_id" AS episode_of_care_id,
    "start_date" AS start_date,
    "end_date" AS end_date
FROM {{ source('OLIDS_MASKED', 'PATIENT_REGISTERED_PRACTITIONER_IN_ROLE') }}
