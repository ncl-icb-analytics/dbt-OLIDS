/*
CONCEPT_MAP Base View
OLIDS terminology concept mappings from CONCEPT_MAP source.
Passthrough view with standard column naming applied.
*/

SELECT
    "id" AS id,
    "lds_id" AS lds_id,
    "lds_business_key" AS lds_business_key,
    "lds_dataset_id" AS lds_dataset_id,
    "concept_map_id" AS concept_map_id,
    "source_code_id" AS source_code_id,
    "target_code_id" AS target_code_id,
    "is_primary" AS is_primary,
    "equivalence" AS equivalence,
    "lds_start_date_time" AS lds_start_date_time
FROM {{ source('olids_terminology', 'CONCEPT_MAP') }}
