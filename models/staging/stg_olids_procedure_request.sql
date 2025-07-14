-- Staging model for OLIDS_MASKED.PROCEDURE_REQUEST
-- Source: "Data_Store_OLIDS_UAT".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_dataset_id" AS lds_dataset_id,
    "lds_start_date_time" AS lds_start_date_time,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "person_id" AS person_id,
    "patient_id" AS patient_id,
    "encounter_id" AS encounter_id,
    "practitioner_id" AS practitioner_id,
    "clinical_effective_date" AS clinical_effective_date,
    "date_precision_concept_id" AS date_precision_concept_id,
    "date_recorded" AS date_recorded,
    "description" AS description,
    "procedure_core_concept_id" AS procedure_source_concept_id,
    "status_concept_id" AS status_concept_id,
    "age_at_event" AS age_at_event,
    "age_at_event_baby" AS age_at_event_baby,
    "age_at_event_neonate" AS age_at_event_neonate,
    "is_confidential" AS is_confidential,
    "lds_end_date_time" AS lds_end_date_time
FROM {{ source('OLIDS_MASKED', 'PROCEDURE_REQUEST') }}
