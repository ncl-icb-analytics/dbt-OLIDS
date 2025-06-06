-- Staging model for OLIDS_MASKED.PRACTITIONER
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_MASKED

select
    "lds_id" as lds_id,
    "id" as id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "lds_datetime_data_acquired" as lds_datetime_data_acquired,
    "lds_start_date_time" as lds_start_date_time,
    "record_owner_organisation_code" as record_owner_organisation_code,
    "gmc_code" as gmc_code,
    "title" as title,
    "first_name" as first_name,
    "last_name" as last_name,
    "name" as name,
    "is_obsolete" as is_obsolete,
    "lds_end_date_time" as lds_end_date_time
from {{ source('OLIDS_MASKED', 'PRACTITIONER') }}
