/*
CONCEPT Base View
OLIDS terminology concepts from CONCEPT source.
Passthrough view with standard column naming applied.
*/

SELECT
    id,
    lds_id,
    lds_business_key,
    lds_dataset_id,
    system,
    code,
    display,
    is_mapped,
    use_count,
    lds_start_date_time
FROM {{ source('olids_terminology', 'CONCEPT') }}
