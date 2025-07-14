-- Staging model for OLIDS_MASKED.ENCOUNTER
-- Source: "Data_Store_OLIDS_UAT".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_initial_data_received_date" AS lds_initial_data_received_date,
    "lds_dataset_id" AS lds_dataset_id,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "person_id" AS person_id,
    "patient_id" AS patient_id,
    "practitioner_id" AS practitioner_id,
    "appointment_id" AS appointment_id,
    "episode_of_care_id" AS episode_of_care_id,
    "service_provider_organisation_id" AS service_provider_organisation_id,
    "clinical_effective_date" AS clinical_effective_date,
    "date_precision_concept_id" AS date_precision_concept_id,
    "location" AS location,
    "encounter_source_concept_id" AS encounter_source_concept_id,
    "age_at_event" AS age_at_event,
    "age_at_event_baby" AS age_at_event_baby,
    "age_at_event_neonate" AS age_at_event_neonate,
    "type" AS type,
    "sub_type" AS sub_type,
    "admission_method" AS admission_method,
    "end_date" AS end_date,
    "date_recorded" AS date_recorded,
    "lds_start_date_time" AS lds_start_date_time,
    "lds_end_date_time" AS lds_end_date_time
FROM {{ source('OLIDS_MASKED', 'ENCOUNTER') }}
