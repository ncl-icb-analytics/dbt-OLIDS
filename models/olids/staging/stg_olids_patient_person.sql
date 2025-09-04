-- Staging model for olids_core.PATIENT_PERSON
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data
-- 
-- INTERIM FIX: Generate reliable person_id from sk_patient_id due to duplicate person_id in source
-- See issue #192: https://github.com/ncl-icb-analytics/dbt-OLIDS/issues/192

{{
    config(
        materialized='table'
    )
}}

select distinct
    pp."lds_id" as lds_id,
    pp."id" as id,
    pp."lds_business_key" as lds_business_key,
    pp."lds_datetime_data_acquired" as lds_datetime_data_acquired,
    pp."lds_start_date_time" as lds_start_date_time,
    pp."lds_end_date_time" as lds_end_date_time,
    pp."lds_dataset_id" as lds_dataset_id,
    pp."patient_id" as patient_id,
    p."sk_patient_id" as sk_patient_id,
    -- Generate deterministic person_id from sk_patient_id hash for consistency
    'person-' || MD5(p."sk_patient_id") as person_id,
    -- Keep original person_id for rollback when source is fixed
    pp."person_id" as original_person_id
from {{ source('olids_core', 'PATIENT_PERSON') }} pp
inner join {{ source('olids_core', 'PATIENT') }} p
    on pp."patient_id" = p."id"
where pp."patient_id" is not null 
    and p."sk_patient_id" is not null
