{{
    config(
        secure=true,
        alias='appointment')
}}

/*
Base APPOINTMENT View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
*/

SELECT
    src."LakehouseDateProcessed",
    src."LakehouseDateTimeUpdated",
    src."lds_record_id",
    src."lds_id",
    src."id",
    src."lds_business_key",
    src."lds_dataset_id",
    src."record_owner_organisation_code",
    src."lds_datetime_data_acquired",
    src."lds_initial_data_received_date",
    src."lds_start_date_time",
    src."organisation_id",
    src."patient_id",
    src."practitioner_in_role_id",
    src."schedule_id",
    src."start_date",
    src."planned_duration",
    src."actual_duration",
    src."appointment_status_concept_id",
    src."patient_wait",
    src."patient_delay",
    src."date_time_booked",
    src."date_time_sent_in",
    src."date_time_left",
    src."cancelled_date",
    src."type",
    src."age_at_event",
    src."age_at_event_baby",
    src."age_at_event_neonate",
    src."booking_method_concept_id",
    src."contact_mode_concept_id",
    src."is_blocked",
    src."national_slot_category_name",
    src."context_type",
    src."service_setting",
    src."national_slot_category_description",
    src."csds_care_contact_identifier",
    src."person_id"
FROM {{ source('olids_core', 'APPOINTMENT') }} src
INNER JOIN {{ ref('base_olids_patient') }} patients
    ON src."patient_id" = patients."id"
INNER JOIN {{ ref('int_ncl_practices') }} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code