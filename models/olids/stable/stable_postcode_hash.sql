{{
    config(
        materialized='incremental',
        unique_key='postcode_hash',
        on_schema_change='fail',
        cluster_by=['postcode_hash'],
        alias='postcode_hash',
        incremental_strategy='merge',
        tags=['stable', 'incremental']
    )
}}

select
    postcode_hash,
    primary_care_organisation,
    local_authority_organisation,
    yr_2011_lsoa,
    yr_2011_msoa,
    yr_2021_lsoa,
    yr_2021_msoa,
    effective_from,
    effective_to,
    is_latest,
    lds_start_date_time
from {{ ref('base_olids_postcode_hash') }}

{% if is_incremental() %}
    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})
{% endif %}
