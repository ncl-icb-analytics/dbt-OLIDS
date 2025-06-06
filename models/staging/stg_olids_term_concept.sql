-- Staging model for OLIDS_TERMINOLOGY.CONCEPT
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY

select
    "id" as id,
    "lds_id" as lds_id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "system" as system,
    "code" as code,
    "display" as display,
    "is_mapped" as is_mapped,
    "use_count" as use_count,
    "lds_start_date_time" as lds_start_date_time
from {{ source('OLIDS_TERMINOLOGY', 'CONCEPT') }}
