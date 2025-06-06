-- Staging model for OLIDS_MASKED.PATIENT_PERSON
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_start_date_time" as lds_start_date_time,
    "lds_end_date_time" as lds_end_date_time,
    "lds_dataset_id" as lds_dataset_id,
    "patient_id" as patient_id,
    "person_id" as person_id
from {{ source('OLIDS_MASKED', 'PATIENT_PERSON') }}
