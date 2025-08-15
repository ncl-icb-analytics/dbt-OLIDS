-- Staging model for olids_core.PATIENT_ADDRESS
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_start_date_time" as lds_start_date_time,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "patient_id" as patient_id,
    "address_type_concept_id" as address_type_concept_id,
    "post_code_hash" as post_code_hash,
    "start_date" as start_date,
    "end_date" as end_date,
    "lds_end_date_time" as lds_end_date_time,
    "person_id" as person_id
from {{ source('olids_core', 'PATIENT_ADDRESS') }}
