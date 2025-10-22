{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['diagnostic_order_source_concept_id', 'clinical_effective_date'],
        alias='diagnostic_order',
        incremental_strategy='merge',
        tags=['stable', 'incremental']
    )
}}

select
    lds_record_id,
    id,
    patient_id,
    encounter_id,
    practitioner_id,
    parent_observation_id,
    clinical_effective_date,
    date_precision_concept_id,
    result_value,
    result_value_units_concept_id,
    result_date,
    result_text,
    is_problem,
    is_review,
    problem_end_date,
    diagnostic_order_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    episodicity_concept_id,
    is_primary,
    date_recorded,
    person_id,
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
from {{ ref('base_olids_diagnostic_order') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}