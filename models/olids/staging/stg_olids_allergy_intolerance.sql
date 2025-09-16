-- Staging model for olids_core.ALLERGY_INTOLERANCE
-- Base layer: base_olids_allergy_intolerance (filtered for NCL practices, excludes sensitive patients)
-- Description: Core OLIDS patient and clinical data

select
    lds_record_id,
    id,
    patient_id,
    practitioner_id,
    encounter_id,
    clinical_status,
    verification_status,
    category,
    clinical_effective_date,
    date_precision_concept_id,
    is_review,
    medication_name,
    multi_lex_action,
    allergy_intolerance_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    date_recorded,
    is_confidential,
    person_id,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    lds_cdm_event_id,
    lds_versioner_event_id,
    record_owner_organisation_code,
    lds_datetime_data_acquired,
    lds_initial_data_received_date,
    lds_is_deleted,
    lds_start_date_time,
    lds_lakehouse_date_processed,
    lds_lakehouse_datetime_updated
from {{ ref('base_olids_allergy_intolerance') }}
