{{
    config(
        secure=true,
        alias='encounter')
}}

/*
Base ENCOUNTER View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Note: person_id replaced with fabricated version from patient_person mapping
*/

SELECT
    src."lds_record_id" AS lds_record_id,
    src."id" AS id,
    pp.person_id AS person_id,
    src."patient_id" AS patient_id,
    src."practitioner_id" AS practitioner_id,
    src."appointment_id" AS appointment_id,
    src."episode_of_care_id" AS episode_of_care_id,
    src."service_provider_organisation_id" AS service_provider_organisation_id,
    src."clinical_effective_date" AS clinical_effective_date,
    src."date_precision_concept_id" AS date_precision_concept_id,
    src."location" AS location,
    src."encounter_source_concept_id" AS encounter_source_concept_id,
    src."age_at_event" AS age_at_event,
    src."age_at_event_baby" AS age_at_event_baby,
    src."age_at_event_neonate" AS age_at_event_neonate,
    src."type" AS type,
    src."sub_type" AS sub_type,
    src."admission_method" AS admission_method,
    src."end_date" AS end_date,
    src."date_recorded" AS date_recorded,
    src."is_deleted" AS is_deleted,
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
FROM {{ source('olids_core', 'ENCOUNTER') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients.id
INNER JOIN {{ ref('base_olids_patient_person') }} pp
    ON src."patient_id" = pp.patient_id
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code
WHERE src."lds_start_date_time" IS NOT NULL