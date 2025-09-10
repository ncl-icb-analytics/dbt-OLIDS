-- Staging model for olids_core.ALLERGY_INTOLERANCE
-- Source: "Data_Store_OLIDS_Alpha"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data

select
    "LakehouseDateProcessed" as lakehousedateprocessed,
    "LakehouseDateTimeUpdated" as lakehousedatetimeupdated,
    "lds_record_id" as lds_record_id,
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_initial_data_received_date" as lds_initial_data_received_date,
    "lds_start_date_time" as lds_start_date_time,
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
    "allergy_intolerance_source_concept_id" as allergy_intolerance_source_concept_id,
    "age_at_event" as age_at_event,
    "age_at_event_baby" as age_at_event_baby,
    "age_at_event_neonate" as age_at_event_neonate,
    "date_recorded" as date_recorded,
    "is_confidential" as is_confidential,
    "person_id" as person_id
from {{ source('olids_core', 'ALLERGY_INTOLERANCE') }}
