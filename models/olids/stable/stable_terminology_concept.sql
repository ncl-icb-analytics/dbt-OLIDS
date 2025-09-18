{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['id'],
        alias='terminology_concept',
        incremental_strategy='merge',
        tags=['stable', 'incremental']
    )
}}

select
    id,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    system,
    code,
    display,
    is_mapped,
    use_count,
    lds_start_date_time
from {{ ref('base_olids_terminology_concept') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}