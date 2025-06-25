{{
    config(
        materialized='table',
        tags=['dimension', 'practice', 'geography'],
        cluster_by=['practice_code'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Dimension table providing practice neighbourhood information. Sources from PRACTICE_NEIGHBOURHOOD_LOOKUP in the POPULATION_HEALTH schema. PCN information is available separately in DIM_PRACTICE_PCN.'"
        ]
    )
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
