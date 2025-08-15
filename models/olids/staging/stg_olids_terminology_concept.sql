-- Staging model for olids_terminology.CONCEPT
-- Source: "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"
-- Description: OLIDS-specific terminology and code mappings

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
from {{ source('olids_terminology', 'CONCEPT') }}
