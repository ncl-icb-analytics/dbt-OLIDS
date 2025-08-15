-- Staging model for olids_core.REFERRAL_REQUEST_BACKUP
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_data_initial_received_date" as lds_data_initial_received_date,
    "lds_dataset_id" as lds_dataset_id,
    "lds_start_date_time" as lds_start_date_time,
    "lds_end_date_time" as lds_end_date_time,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "organisation_id" as organisation_id,
    "person_id" as person_id,
    "patient_id" as patient_id,
    "encounter_id" as encounter_id,
    "practitioner_id" as practitioner_id,
    "unique_booking_reference_number" as unique_booking_reference_number,
    "clinical_effective_date" as clinical_effective_date,
    "date_precision_concept_id" as date_precision_concept_id,
    "requester_organisation_id" as requester_organisation_id,
    "recipient_organisation_id" as recipient_organisation_id,
    "referral_request_priority_concept_id" as referral_request_priority_concept_id,
    "referal_request_type_concept_id" as referal_request_type_concept_id,
    "referral_request_specialty_concept_id" as referral_request_specialty_concept_id,
    "mode" as mode,
    "is_outgoing_referral" as is_outgoing_referral,
    "is_review" as is_review,
    "referral_request_core_concept_id" as referral_request_core_concept_id,
    "referral_request_raw_concept_id" as referral_request_raw_concept_id,
    "age_at_event" as age_at_event,
    "age_at_event_baby" as age_at_event_baby,
    "age_at_event_neonate" as age_at_event_neonate,
    "date_recorded" as date_recorded
from {{ source('olids_core', 'REFERRAL_REQUEST_BACKUP') }}
