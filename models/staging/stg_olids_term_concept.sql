-- Staging model for OLIDS_TERMINOLOGY.CONCEPT
-- Source: "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY

SELECT
    "id" AS id,
    "lds_id" AS lds_id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "system" AS system,
    "code" AS code,
    "display" AS display,
    "is_mapped" AS is_mapped,
    "use_count" AS use_count,
    "lds_start_date_time" AS lds_start_date_time
FROM {{ source('OLIDS_TERMINOLOGY', 'CONCEPT') }}
