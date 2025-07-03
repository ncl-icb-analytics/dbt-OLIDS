{{
    config(
        materialized='table',
        tags=['dimension', 'practice', 'pcn'],
        cluster_by=['practice_code'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Mart: Practice PCN Dimension - Primary Care Network mapping for organisational hierarchy and network analysis.

Business Purpose:
• Support operational analytics for Primary Care Network management and practice collaboration
• Enable business intelligence reporting on PCN-level performance and resource allocation
• Provide foundation for network-based service planning and commissioning
• Support population health analytics at PCN level for integrated care delivery

Data Granularity:
• One row per practice with PCN assignment
• Includes practice to PCN mapping for organisational hierarchy
• Current PCN network structure and practice affiliations

Key Features:
• Links practices to Primary Care Networks for network-level analysis
• Supports PCN-level reporting and performance monitoring
• Enables business intelligence for network operations and collaborative care
• Provides foundation for integrated care delivery and network resource planning'"
        ]
    )
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
