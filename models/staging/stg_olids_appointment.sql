-- Staging model for OLIDS_MASKED.APPOINTMENT
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

SELECT
    "lds_id" AS lds_id,
    "id" AS id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "record_owner_organisation_code" AS record_owner_organisation_code,
    "lds_datetime_data_acquired" AS lds_datetime_data_acquired,
    "lds_initial_data_received_date" AS lds_initial_data_received_date,
    "lds_start_date_time" AS lds_start_date_time,
    "organisation_id" AS organisation_id,
    "person_id" AS person_id,
    "patient_id" AS patient_id,
    "practitioner_in_role_id" AS practitioner_in_role_id,
    "schedule_id" AS schedule_id,
    "start_date" AS start_date,
    "planned_duration" AS planned_duration,
    "actual_duration" AS actual_duration,
    "appointment_status_concept_id" AS appointment_status_concept_id,
    "patient_wait" AS patient_wait,
    "patient_delay" AS patient_delay,
    "date_time_booked" AS date_time_booked,
    "date_time_sent_in" AS date_time_sent_in,
    "date_time_left" AS date_time_left,
    "cancelled_date" AS cancelled_date,
    "type" AS type,
    "age_at_event" AS age_at_event,
    "age_at_event_baby" AS age_at_event_baby,
    "age_at_event_neonate" AS age_at_event_neonate,
    "booking_method_concept_id" AS booking_method_concept_id,
    "contact_mode_concept_id" AS contact_mode_concept_id
FROM {{ source('OLIDS_MASKED', 'APPOINTMENT') }}
