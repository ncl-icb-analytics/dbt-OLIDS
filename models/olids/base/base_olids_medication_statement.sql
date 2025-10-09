{{
    config(
        secure=true,
        alias='medication_statement')
}}

/*
Base MEDICATION_STATEMENT View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Note: person_id replaced with fabricated version from patient_person mapping
*/

SELECT
    src."lds_record_id" AS lds_record_id,
    src."id" AS id,
    src."organisation_id" AS organisation_id,
    pp.person_id AS person_id,
    src."patient_id" AS patient_id,
    src."encounter_id" AS encounter_id,
    src."practitioner_id" AS practitioner_id,
    src."observation_id" AS observation_id,
    src."allergy_intolerance_id" AS allergy_intolerance_id,
    src."diagnostic_order_id" AS diagnostic_order_id,
    src."referral_request_id" AS referral_request_id,
    src."authorisation_type_concept_id" AS authorisation_type_concept_id,
    src."date_precision_concept_id" AS date_precision_concept_id,
    src."medication_statement_source_concept_id" AS medication_statement_source_concept_id,
    src."clinical_effective_date" AS clinical_effective_date,
    src."cancellation_date" AS cancellation_date,
    src."dose" AS dose,
    src."quantity_value_description" AS quantity_value_description,
    src."quantity_value" AS quantity_value,
    src."quantity_unit" AS quantity_unit,
    src."medication_name" AS medication_name,
    src."bnf_reference" AS bnf_reference,
    src."age_at_event" AS age_at_event,
    src."age_at_event_baby" AS age_at_event_baby,
    src."age_at_event_neonate" AS age_at_event_neonate,
    src."issue_method" AS issue_method,
    src."date_recorded" AS date_recorded,
    src."is_active" AS is_active,
    src."is_confidential" AS is_confidential,
    src."is_deleted" AS is_deleted,
    src."expiry_date" AS expiry_date,
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
FROM {{ source('olids_core', 'MEDICATION_STATEMENT') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients.id
INNER JOIN {{ ref('base_olids_patient_person') }} pp
    ON src."patient_id" = pp.patient_id
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code
WHERE src."medication_statement_source_concept_id" IS NOT NULL
    AND src."lds_start_date_time" IS NOT NULL