-- Staging model for patient-person relationships
-- Generated from PATIENT table rather than PATIENT_PERSON due to data quality issues
-- See issue #192: https://github.com/ncl-icb-analytics/dbt-OLIDS/issues/192
-- 
-- Mirrors exact PATIENT_PERSON structure but generates from PATIENT table

{{
    config(
        materialized='table'
    )
}}

select
    -- New Alpha columns first (following PATIENT_PERSON structure order)
    p."LakehouseDateProcessed" as lakehousedateprocessed,
    p."LakehouseDateTimeUpdated" as lakehousedatetimeupdated,
    p."lds_record_id" as lds_record_id,
    -- Generate deterministic lds_id for patient_person bridge
    'pp-' || MD5(p."sk_patient_id") as lds_id,
    -- Generate deterministic id for this bridge record
    'pp-' || MD5(p."sk_patient_id") as id,
    p."lds_datetime_data_acquired" as lds_datetime_data_acquired,
    p."lds_start_date_time" as lds_start_date_time,
    p."lds_end_date_time" as lds_end_date_time,
    p."lds_dataset_id" as lds_dataset_id,
    p."id" as patient_id,
    -- Generate deterministic person_id from sk_patient_id
    'person-' || MD5(p."sk_patient_id") as person_id
from {{ source('olids_core', 'PATIENT') }} p
where p."sk_patient_id" is not null
    and LENGTH(TRIM(p."sk_patient_id")) > 0  -- Ensure sk_patient_id is not empty after trimming
    and p."is_dummy_patient" = false
