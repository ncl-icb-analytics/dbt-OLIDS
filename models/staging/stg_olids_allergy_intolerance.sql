-- Staging model for OLIDS_MASKED.ALLERGY_INTOLERANCE
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_initial_data_received_date" AS lds_initial_data_received_date,
    "lds_start_date_time" AS lds_start_date_time,
    "person_id" AS person_id,
    "patient_id" AS patient_id,
    "practitioner_id" AS practitioner_id,
    "encounter_id" AS encounter_id,
    "clinical_status" AS clinical_status,
    "verification_status" AS verification_status,
    "category" AS category,
    "clinical_effective_date" AS clinical_effective_date,
    "date_precision_concept_id" AS date_precision_concept_id,
    "is_review" AS is_review,
    "medication_name" AS medication_name,
    "multi_lex_action" AS multi_lex_action,
    "allergy_intolerance_core_concept_id"
        AS allergy_intolerance_core_concept_id,
    "allergy_intolerance_raw_concept_id" AS allergy_intolerance_raw_concept_id,
    "age_at_event" AS age_at_event,
    "age_at_event_baby" AS age_at_event_baby,
    "age_at_event_neonate" AS age_at_event_neonate,
    "date_recorded" AS date_recorded,
    "is_confidential" AS is_confidential
FROM {{ source('OLIDS_MASKED', 'ALLERGY_INTOLERANCE') }}
