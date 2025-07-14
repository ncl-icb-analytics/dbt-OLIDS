-- Staging model for OLIDS_MASKED.EPISODE_OF_CARE
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
    "organisation_id" AS organisation_id,
    "patient_id" AS patient_id,
    "person_id" AS person_id,
    "episode_type_source_concept_id" AS episode_type_source_concept_id,
    "episode_status_source_concept_id" AS episode_status_source_concept_id,
    "episode_of_care_start_date" AS episode_of_care_start_date,
    "episode_of_care_end_date" AS episode_of_care_end_date,
    "care_manager_practitioner_id" AS care_manager_practitioner_id
FROM {{ source('OLIDS_MASKED', 'EPISODE_OF_CARE') }}
