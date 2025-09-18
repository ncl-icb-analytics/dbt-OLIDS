{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['medication_statement_source_concept_id', 'clinical_effective_date'],
        alias='medication_statement',
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
    observation_id,
    allergy_intolerance_id,
    diagnostic_order_id,
    referral_request_id,
    authorisation_type_concept_id,
    date_precision_concept_id,
    medication_statement_source_concept_id,
    clinical_effective_date,
    cancellation_date,
    dose,
    quantity_value_description,
    quantity_value,
    quantity_unit,
    medication_name,
    bnf_reference,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    issue_method,
    date_recorded,
    is_active,
    is_confidential,
    is_deleted,
    expiry_date,
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
from {{ ref('base_olids_medication_statement') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}