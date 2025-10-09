{{
    config(
        secure=true,
        alias='appointment')
}}

/*
Base APPOINTMENT View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
Note: person_id replaced with fabricated version from patient_person mapping
*/

SELECT
    src."lds_record_id" AS lds_record_id,
    src."id" AS id,
    src."organisation_id" AS organisation_id,
    src."patient_id" AS patient_id,
    src."practitioner_in_role_id" AS practitioner_in_role_id,
    src."schedule_id" AS schedule_id,
    src."start_date" AS start_date,
    src."planned_duration" AS planned_duration,
    src."actual_duration" AS actual_duration,
    src."appointment_status_concept_id" AS appointment_status_concept_id,
    src."patient_wait" AS patient_wait,
    src."patient_delay" AS patient_delay,
    src."date_time_booked" AS date_time_booked,
    src."date_time_sent_in" AS date_time_sent_in,
    src."date_time_left" AS date_time_left,
    src."cancelled_date" AS cancelled_date,
    src."type" AS type,
    src."age_at_event" AS age_at_event,
    src."age_at_event_baby" AS age_at_event_baby,
    src."age_at_event_neonate" AS age_at_event_neonate,
    src."booking_method_concept_id" AS booking_method_concept_id,
    src."contact_mode_concept_id" AS contact_mode_concept_id,
    src."is_blocked" AS is_blocked,
    src."national_slot_category_name" AS national_slot_category_name,
    src."context_type" AS context_type,
    src."service_setting" AS service_setting,
    src."national_slot_category_description" AS national_slot_category_description,
    src."csds_care_contact_identifier" AS csds_care_contact_identifier,
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
FROM {{ source('olids_common', 'APPOINTMENT') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients.id
INNER JOIN {{ ref('base_olids_patient_person') }} pp
    ON src."patient_id" = pp.patient_id
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code
WHERE src."patient_id" IS NOT NULL
    AND src."start_date" IS NOT NULL
    AND src."lds_start_date_time" IS NOT NULL