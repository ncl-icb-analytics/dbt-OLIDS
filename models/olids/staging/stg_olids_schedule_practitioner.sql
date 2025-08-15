-- Staging model for olids_core.SCHEDULE_PRACTITIONER
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_MASKED"
-- Description: Core OLIDS patient and clinical data

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_start_date_time" as lds_start_date_time,
    "schedule_id" as schedule_id,
    "practitioner_id" as practitioner_id
from {{ source('olids_core', 'SCHEDULE_PRACTITIONER') }}
