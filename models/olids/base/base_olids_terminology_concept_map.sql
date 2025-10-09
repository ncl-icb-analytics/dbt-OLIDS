{{
    config(
        secure=true,
        alias='concept_map')
}}

/*
Base CONCEPT_MAP View
Terminology data with unquoted identifiers.
Pattern: Terminology table from OLIDS_TERMINOLOGY schema
*/

SELECT
    src."id" AS id,
    src."lds_id" AS lds_id,
    src."lds_business_key" AS lds_business_key,
    src."lds_dataset_id" AS lds_dataset_id,
    src."concept_map_id" AS concept_map_id,
    src."source_code_id" AS source_code_id,
    src."target_code_id" AS target_code_id,
    src."is_primary" AS is_primary,
    src."equivalence" AS equivalence,
    src."lds_start_date_time" AS lds_start_date_time
FROM {{ source('olids_terminology', 'CONCEPT_MAP') }} src
