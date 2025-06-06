-- Staging model for OLIDS_TERMINOLOGY.CONCEPT_MAP
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY

select
    "id" as id,
    "lds_id" as lds_id,
    "lds_business_key" as lds_business_key,
    "lds_dataset_id" as lds_dataset_id,
    "concept_map_id" as concept_map_id,
    "source_code_id" as source_code_id,
    "target_code_id" as target_code_id,
    "is_primary" as is_primary,
    "equivalence" as equivalence,
    "lds_start_date_time" as lds_start_date_time
from {{ source('OLIDS_TERMINOLOGY', 'CONCEPT_MAP') }}
