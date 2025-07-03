{{
    config(
        materialized='table',
        tags=['dimension', 'practice', 'geography'],
        cluster_by=['practice_code'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Mart: Practice Neighbourhood Dimension - Geographic context and neighbourhood classification for GP practices.

Business Purpose:
• Support operational analytics for practice geographic distribution and catchment area analysis
• Enable business intelligence reporting on practice locations and local authority contexts
• Provide foundation for population health analytics and geographic health inequalities assessment
• Support commissioning and service planning based on practice geographic characteristics

Data Granularity:
• One row per practice with neighbourhood and local authority information
• Includes practice geographic context for spatial analysis
• Current geographic classification and neighbourhood assignment

Key Features:
• Links practices to local authorities and neighbourhood classifications
• Supports geographic analysis and spatial reporting for population health
• Enables business intelligence for practice operations and service planning
• Provides foundation for health inequalities analysis and resource allocation'"
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
