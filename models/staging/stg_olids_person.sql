-- Staging model for OLIDS_MASKED.PERSON
-- Source: "Data_Store_OLIDS_UAT".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_dataset_id" AS lds_dataset_id,
    "LDSBusinessId_PrimaryPatient" AS ldsbusinessid_primarypatient,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "requesting_patient_record_id" AS requesting_patient_record_id,
    "unique_reference" AS unique_reference,
    "requesting_nhs_numberhash" AS requesting_nhs_numberhash,
    "errror_success_code" AS errror_success_code,
    "matched_nhs_numberhash" AS matched_nhs_numberhash,
    "sensitivity_flag" AS sensitivity_flag,
    "matched_algorithm_indicator" AS matched_algorithm_indicator,
    "requesting_patient_id" AS requesting_patient_id,
    "lds_start_date_time" AS lds_start_date_time,
    "lds_end_date_time" AS lds_end_date_time
FROM {{ source('OLIDS_MASKED', 'PERSON') }}
