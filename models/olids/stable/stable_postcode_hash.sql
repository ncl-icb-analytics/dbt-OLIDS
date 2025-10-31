{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail',
        cluster_by=['id', 'postcode_hash'],
        alias='postcode_hash',
        incremental_strategy='merge',
        tags=['stable', 'incremental']
    )
}}

select
    id,
    postcode_hash,
    primary_care_organisation,
    local_authority_organisation,
    yr2011_lsoa,
    yr2011_msoa,
    yr2021_lsoa,
    yr2021_msoa,
    effective_from,
    effective_to,
    is_latest,
    lds_is_deleted,
    lds_start_date_time,
    lakehousedateprocessed,
    high_watermark_date_time
from {{ ref('base_olids_postcode_hash') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}
