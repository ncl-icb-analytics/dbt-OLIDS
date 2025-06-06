-- Staging model for OLIDS_MASKED.ALLERGY_INTOLERANCE
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_initial_data_received_date" as lds_initial_data_received_date,
    "lds_start_date_time" as lds_start_date_time,
    "person_id" as person_id,
    "patient_id" as patient_id,
    "practitioner_id" as practitioner_id,
    "encounter_id" as encounter_id,
    "clinical_status" as clinical_status,
    "verification_status" as verification_status,
    "category" as category,
    "clinical_effective_date" as clinical_effective_date,
    "date_precision_concept_id" as date_precision_concept_id,
    "is_review" as is_review,
    "medication_name" as medication_name,
    "multi_lex_action" as multi_lex_action,
    "allergy_intolerance_core_concept_id" as allergy_intolerance_core_concept_id,
    "allergy_intolerance_raw_concept_id" as allergy_intolerance_raw_concept_id,
    "age_at_event" as age_at_event,
    "age_at_event_baby" as age_at_event_baby,
    "age_at_event_neonate" as age_at_event_neonate,
    "date_recorded" as date_recorded,
    "is_confidential" as is_confidential
from {{ source('OLIDS_MASKED', 'ALLERGY_INTOLERANCE') }}
