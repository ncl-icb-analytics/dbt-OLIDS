{{ config(materialized='view') }}

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_start_date_time" as lds_start_date_time,
    "lds_end_date_time" as lds_end_date_time,
    "lds_dataset_id" as lds_dataset_id,
    "patient_id" as patient_id,
    "person_id" as person_id
from {{ source('olids_masked', 'PATIENT_PERSON') }}