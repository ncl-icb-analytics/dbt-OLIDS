{{
    config(
        secure=true,
        alias='concept')
}}

/*
Base CONCEPT View
Terminology data with unquoted identifiers.
Pattern: Terminology table from OLIDS_TERMINOLOGY schema
*/

SELECT
    src."id" AS id,
    src."lds_id" AS lds_id,
    src."lds_business_key" AS lds_business_key,
    src."lds_dataset_id" AS lds_dataset_id,
    src."system" AS system,
    src."code" AS code,
    src."display" AS display,
    src."is_mapped" AS is_mapped,
    src."use_count" AS use_count,
    src."lds_start_date_time" AS lds_start_date_time
FROM {{ source('olids_terminology', 'CONCEPT') }} src