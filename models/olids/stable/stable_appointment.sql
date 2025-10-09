{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['start_date', 'patient_id'],
        alias='appointment',
        incremental_strategy='merge',
        tags=['stable', 'incremental']
    )
}}

select
    lds_record_id,
    id,
    organisation_id,
    patient_id,
    person_id,
    practitioner_in_role_id,
    schedule_id,
    start_date,
    planned_duration,
    actual_duration,
    appointment_status_concept_id,
    patient_wait,
    patient_delay,
    date_time_booked,
    date_time_sent_in,
    date_time_left,
    cancelled_date,
    type,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    booking_method_concept_id,
    contact_mode_concept_id,
    is_blocked,
    national_slot_category_name,
    context_type,
    service_setting,
    national_slot_category_description,
    csds_care_contact_identifier,
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
from {{ ref('base_olids_appointment') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}