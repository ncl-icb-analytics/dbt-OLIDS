-- Staging model for olids_core.FLAG
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_initial_data_received_date" as lds_initial_data_received_date,
    "lds_start_date_time" as lds_start_date_time,
    "person_id" as person_id,
    "patient_id" as patient_id,
    "effective_date" as effective_date,
    "expired_date" as expired_date,
    "is_active" as is_active,
    "flag_text" as flag_text
from {{ source('olids_core', 'FLAG') }}
