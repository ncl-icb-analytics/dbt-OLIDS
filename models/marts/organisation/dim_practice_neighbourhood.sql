{{
    config(
        materialized='table',
        tags=['dimension', 'practice', 'geography'],
        cluster_by=['practice_code'])
}}

/*
Practice Neighbourhood Dimension
Provides geographic context for GP practices including local authority and neighbourhood classification.
Note: Working with dummy data so geographic information may be limited/placeholder.
*/

SELECT
    practice_code,
    practice_name,
    local_authority,
    practice_neighbourhood
FROM {{ ref('stg_population_health_practice_neighbourhood_lookup') }}
WHERE practice_code IS NOT NULL
