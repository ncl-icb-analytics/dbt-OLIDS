-- Staging model for OLIDS_MASKED.PERSON
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "primary_patient_id" as primary_patient_id,
    "lds_start_date_time" as lds_start_date_time,
    "lds_end_date_time" as lds_end_date_time
from {{ source('OLIDS_MASKED', 'PERSON') }}
