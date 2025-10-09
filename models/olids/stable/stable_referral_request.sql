{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['referral_request_source_concept_id', 'clinical_effective_date'],
        alias='referral_request',
        incremental_strategy='merge',
        tags=['stable', 'incremental']
    )
}}

select
    lds_record_id,
    id,
    organisation_id,
    person_id,
    patient_id,
    encounter_id,
    practitioner_id,
    unique_booking_reference_number,
    clinical_effective_date,
    date_precision_concept_id,
    requester_organisation_id,
    recipient_organisation_id,
    referral_request_priority_concept_id,
    referral_request_type_concept_id,
    referral_request_specialty_concept_id,
    mode,
    is_outgoing_referral,
    is_review,
    referral_request_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    date_recorded,
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
from {{ ref('base_olids_referral_request') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}