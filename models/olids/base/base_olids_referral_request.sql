{{
    config(
        secure=true,
        alias='referral_request')
}}

/*
Base REFERRAL_REQUEST View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Note: person_id replaced with fabricated version from patient_person mapping
*/

SELECT
    src.lds_record_id,
    src.id,
    src.organisation_id,
    pp.person_id,
    src.patient_id,
    src.encounter_id,
    src.practitioner_id,
    src.unique_booking_reference_number,
    src.clinical_effective_date,
    src.date_precision_concept_id,
    src.requester_organisation_id,
    src.recipient_organisation_id,
    src.referral_request_priority_concept_id,
    src.referral_request_type_concept_id,
    src.referral_request_specialty_concept_id,
    src.mode,
    src.is_outgoing_referral,
    src.is_review,
    src.referral_request_source_concept_id,
    src.age_at_event,
    src.age_at_event_baby,
    src.age_at_event_neonate,
    src.date_recorded,
    src.lds_id,
    src.lds_business_key,
    src.lds_dataset_id,
    src.lds_cdm_event_id,
    src.lds_versioner_event_id,
    src.record_owner_organisation_code,
    src.lds_datetime_data_acquired,
    src.lds_initial_data_received_date,
    src.lds_is_deleted,
    src.lds_start_date_time,
    src.lds_lakehouse_date_processed,
    src.lds_lakehouse_datetime_updated
FROM {{ source('olids_common', 'REFERRAL_REQUEST') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src.patient_id = patients.id
INNER JOIN {{ ref('base_olids_patient_person') }} pp
    ON src.patient_id = pp.patient_id
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src.record_owner_organisation_code = ncl_practices.practice_code
WHERE src.lds_start_date_time IS NOT NULL