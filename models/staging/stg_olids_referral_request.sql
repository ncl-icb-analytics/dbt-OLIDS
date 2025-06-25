-- Staging model for OLIDS_MASKED.REFERRAL_REQUEST
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_data_initial_received_date" AS lds_data_initial_received_date,
    "lds_dataset_id" AS lds_dataset_id,
    "lds_start_date_time" AS lds_start_date_time,
    "lds_end_date_time" AS lds_end_date_time,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "organisation_id" AS organisation_id,
    "person_id" AS person_id,
    "patient_id" AS patient_id,
    "encounter_id" AS encounter_id,
    "practitioner_id" AS practitioner_id,
    "unique_booking_reference_number" AS unique_booking_reference_number,
    "clinical_effective_date" AS clinical_effective_date,
    "date_precision_concept_id" AS date_precision_concept_id,
    "requester_organisation_id" AS requester_organisation_id,
    "recipient_organisation_id" AS recipient_organisation_id,
    "referral_request_priority_concept_id"
        AS referral_request_priority_concept_id,
    "referal_request_type_concept_id" AS referal_request_type_concept_id,
    "mode" AS mode,
    "is_outgoing_referral" AS is_outgoing_referral,
    "is_review" AS is_review,
    "referral_request_core_concept_id" AS referral_request_core_concept_id,
    "referral_request_raw_concept_id" AS referral_request_raw_concept_id,
    "age_at_event" AS age_at_event,
    "age_at_event_baby" AS age_at_event_baby,
    "age_at_event_neonate" AS age_at_event_neonate,
    "date_recorded" AS date_recorded
FROM {{ source('OLIDS_MASKED', 'REFERRAL_REQUEST') }}
