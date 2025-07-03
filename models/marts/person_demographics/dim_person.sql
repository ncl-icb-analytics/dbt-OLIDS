{{
    config(
        materialized='table',
        tags=['dimension', 'person'],
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Mart: Person Dimension - Core person identity and practice relationships for population health analytics.

Business Purpose:
• Support population health analytics by providing single person view across multiple patient records
• Enable business intelligence reporting on person-level care coordination and practice relationships
• Provide foundation for longitudinal care analysis and patient journey mapping
• Support operational analytics for person-centered care and practice management

Data Granularity:
• One row per person with aggregated patient IDs and practice relationships
• Includes current and historical practice associations
• Core person identity linking multiple patient records

Key Features:
• Aggregates multiple patient IDs per person for comprehensive care view
• Tracks current and historical practice relationships
• Supports person-centered analytics and longitudinal care assessment
• Enables business intelligence for care coordination and practice operations'"
        ]
    )
}}

-- Person Dimension Table - Simplified
-- Aggregates person-to-patient relationships and practice associations
-- Uses arrays to store multiple patient IDs and practice information per person

WITH person_patients AS (
    -- Get all patient relationships for each person
    SELECT
        pp.person_id,
        ARRAY_AGG(DISTINCT p.sk_patient_id) AS sk_patient_ids,
        ARRAY_AGG(DISTINCT p.id) AS patient_ids,
        COUNT(DISTINCT p.id) AS total_patients
    FROM {{ ref('stg_olids_patient_person') }} AS pp
    INNER JOIN {{ ref('stg_olids_patient') }} AS p
        ON pp.patient_id = p.id
    GROUP BY pp.person_id
),

person_practices AS (
    -- Get all practice relationships from the historical practice dimension
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT practice_id) AS practice_ids,
        ARRAY_AGG(DISTINCT practice_code) AS practice_codes,
        ARRAY_AGG(DISTINCT practice_name) AS practice_names,
        COUNT(DISTINCT practice_id) AS total_practices
    FROM {{ ref('dim_person_historical_practice') }}
    GROUP BY person_id
),

current_practices AS (
    -- Get the current practice for each person
    SELECT
        person_id,
        practice_id AS current_practice_id,
        practice_code AS current_practice_code,
        practice_name AS current_practice_name
    FROM {{ ref('dim_person_historical_practice') }}
    WHERE is_current_registration = TRUE
)

-- Final aggregation
SELECT
    pp.person_id,
    pp.sk_patient_ids,
    pp.patient_ids,
    cp.current_practice_id,
    cp.current_practice_code,
    cp.current_practice_name,
    pp.total_patients,
    COALESCE(pr.practice_ids, ARRAY_CONSTRUCT()) AS practice_ids,
    COALESCE(pr.practice_codes, ARRAY_CONSTRUCT()) AS practice_codes,
    COALESCE(pr.practice_names, ARRAY_CONSTRUCT()) AS practice_names,
    COALESCE(pr.total_practices, 0) AS total_practices
FROM person_patients AS pp
LEFT JOIN person_practices AS pr
    ON pp.person_id = pr.person_id
LEFT JOIN current_practices AS cp
    ON pp.person_id = cp.person_id
