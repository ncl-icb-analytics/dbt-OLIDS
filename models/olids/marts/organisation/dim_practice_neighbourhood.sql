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
    practicecode AS practice_code,
    practicename AS practice_name,
    localauthority AS local_authority,
    practiceneighbourhood AS neighbourhood_registered
FROM {{ ref('stg_reference_practice_neighbourhood_lookup') }}
WHERE practicecode IS NOT NULL
