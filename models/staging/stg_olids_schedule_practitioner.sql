-- Staging model for OLIDS_MASKED.SCHEDULE_PRACTITIONER
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

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
from {{ source('OLIDS_MASKED', 'SCHEDULE_PRACTITIONER') }}
