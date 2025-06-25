-- Staging model for OLIDS_MASKED.APPOINTMENT_PRACTITIONER
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "lds_record_id_user" AS lds_record_id_user,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_initial_data_received_date" AS lds_initial_data_received_date,
    "lds_start_date_time" AS lds_start_date_time,
    "appointment_id" AS appointment_id,
    "practitioner_id" AS practitioner_id,
    "lds_end_date_time" AS lds_end_date_time
FROM {{ source('OLIDS_MASKED', 'APPOINTMENT_PRACTITIONER') }}
