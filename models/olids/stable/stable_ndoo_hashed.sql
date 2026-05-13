{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['nhs_number_hash'],
        alias='ndoo_hashed',
        incremental_strategy='merge',
        transient=false,
        tags=['stable', 'incremental']
    )
}}

select
    id,
    lds_record_id,
    sk_patient_id,
    nhs_number_hash,
    preference_type,
    preference_status,
    lds_is_deleted,
    lds_datetime_data_acquired,
    lds_start_date_time,
    lds_batch_id,
    lds_file_id,
    lds_dataset_id,
    effective_from,
    effective_to,
    is_latest,
    lakehouse_date_processed,
    high_watermark_date_time
from {{ ref('base_olids_ndoo_hashed') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}
