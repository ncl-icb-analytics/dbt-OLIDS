{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['patient_id', 'start_date'],
        alias='patient_registered_practitioner_in_role',
        incremental_strategy='merge',
        tags=['stable', 'incremental']
    )
}}

select
    lds_record_id,
    id,
    person_id,
    patient_id,
    organisation_id,
    practitioner_id,
    episode_of_care_id,
    start_date,
    end_date,
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
from {{ ref('base_olids_patient_registered_practitioner_in_role') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}