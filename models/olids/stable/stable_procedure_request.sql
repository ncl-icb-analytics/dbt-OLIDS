{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['procedure_request_source_concept_id', 'clinical_effective_date'],
        alias='procedure_request',
        incremental_strategy='merge',
        tags=['stable', 'incremental']
    )
}}

select
    lds_record_id,
    id,
    person_id,
    patient_id,
    encounter_id,
    practitioner_id,
    clinical_effective_date,
    date_precision_concept_id,
    date_recorded,
    description,
    procedure_request_source_concept_id,
    status_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    is_confidential,
    lds_end_date_time,
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
from {{ ref('base_olids_procedure_request') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}