{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['person_id', 'patient_id'],
        alias='patient_person',
        incremental_strategy='merge',
        tags=['stable', 'incremental']
    )
}}

select
    lakehousedateprocessed,
    lakehousedatetimeupdated,
    lds_record_id,
    lds_id,
    id,
    lds_datetime_data_acquired,
    lds_start_date_time,
    lds_dataset_id,
    patient_id,
    person_id
from {{ ref('base_olids_patient_person') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}