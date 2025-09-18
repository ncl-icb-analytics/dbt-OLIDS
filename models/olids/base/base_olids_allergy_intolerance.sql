{{
    config(
        secure=true,
        alias='allergy_intolerance')
}}

/*
Base ALLERGY_INTOLERANCE View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Note: person_id replaced with fabricated version from patient_person mapping
*/

SELECT
    src."lds_record_id" AS lds_record_id,
    src."id" AS id,
    src."patient_id" AS patient_id,
    src."practitioner_id" AS practitioner_id,
    src."encounter_id" AS encounter_id,
    src."clinical_status" AS clinical_status,
    src."verification_status" AS verification_status,
    src."category" AS category,
    src."clinical_effective_date" AS clinical_effective_date,
    src."date_precision_concept_id" AS date_precision_concept_id,
    src."is_review" AS is_review,
    src."medication_name" AS medication_name,
    src."multi_lex_action" AS multi_lex_action,
    src."allergy_intolerance_source_concept_id" AS allergy_intolerance_source_concept_id,
    src."age_at_event" AS age_at_event,
    src."age_at_event_baby" AS age_at_event_baby,
    src."age_at_event_neonate" AS age_at_event_neonate,
    src."date_recorded" AS date_recorded,
    src."is_confidential" AS is_confidential,
    pp.person_id AS person_id,
    src."lds_id" AS lds_id,
    src."lds_business_key" AS lds_business_key,
    src."lds_dataset_id" AS lds_dataset_id,
    src."lds_cdm_event_id" AS lds_cdm_event_id,
    src."lds_versioner_event_id" AS lds_versioner_event_id,
    src."record_owner_organisation_code" AS record_owner_organisation_code,
    src."lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    src."lds_initial_data_received_date" AS lds_initial_data_received_date,
    src."lds_is_deleted" AS lds_is_deleted,
    src."lds_start_date_time" AS lds_start_date_time,
    src."lds_lakehouse_date_processed" AS lds_lakehouse_date_processed,
    src."lds_lakehouse_datetime_updated" AS lds_lakehouse_datetime_updated
FROM {{ source('olids_core', 'ALLERGY_INTOLERANCE') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients.id
INNER JOIN {{ ref('base_olids_patient_person') }} pp
    ON src."patient_id" = pp.patient_id
INNER JOIN {{ ref('base_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code