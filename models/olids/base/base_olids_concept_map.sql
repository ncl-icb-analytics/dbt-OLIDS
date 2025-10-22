/*
CONCEPT_MAP Base View
OLIDS terminology concept mappings from CONCEPT_MAP source.
Passthrough view with standard column naming applied.
*/

SELECT
    id,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    concept_map_id,
    source_code_id,
    target_code_id,
    is_primary,
    equivalence,
    lds_start_date_time
FROM {{ source('olids_terminology', 'CONCEPT_MAP') }}
