-- Staging model for person dimension
-- Generated from stg_olids_patient_person to ensure one record per unique person
-- See issue #192: https://github.com/ncl-icb-analytics/dbt-OLIDS/issues/192
--
-- Creates one record per unique person_id from the patient_person bridge

{{
    config(
        materialized='table'
    )
}}

with unique_persons as (
    select distinct
        person_id
    from {{ ref('stg_olids_patient_person') }}
)

select
    -- Generate deterministic lds_id for person records
    'person-lds-' || MD5(person_id) as lds_id,
    person_id as id,
    null::text as lds_dataset_id,
    null::timestamp_ntz as lds_datetime_data_acquired,
    null::timestamp_ntz as lds_start_date_time,
    null::timestamp_ntz as lds_end_date_time,
    null::text as requesting_patient_record_id,
    null::text as unique_reference,
    null::text as requesting_nhs_number_hash,
    null::text as sk_patient_id_request,
    null::text as error_success_code,
    null::text as matched_nhs_numberhash,
    null::text as sk_patient_id_matched,
    null::text as sensitivity_flag,
    null::text as matched_algorithm_indicator,
    null::text as requesting_patient_id
from unique_persons
