{{
    config(
        materialized='table',
        tags=['dimension', 'practice', 'pcn'],
        cluster_by=['practice_code'])
}}

/*
Practice PCN (Primary Care Network) Dimension
Provides practice to PCN mapping for organisational hierarchy.
Uses the practice neighbourhood lookup which contains PCN codes.
*/

SELECT
    practice_code,
    practice_name,
    pcn_code,
    -- Since we don't have separate PCN names in dummy data, use PCN code as name
    COALESCE(pcn_code, 'Unknown PCN') AS pcn_name
FROM {{ ref('stg_population_health_practice_neighbourhood_lookup') }}
WHERE
    practice_code IS NOT NULL
    AND pcn_code IS NOT NULL
