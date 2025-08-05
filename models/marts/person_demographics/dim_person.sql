{{
    config(
        materialized='table',
        tags=['dimension', 'person'],
        cluster_by=['person_id'])
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
    FROM {{ ref('int_patient_person_unique') }} AS pp
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
    -- FIXED: Handle multiple current registrations per person by selecting the most recent
    SELECT
        person_id,
        practice_id AS current_practice_id,
        practice_code AS current_practice_code,
        practice_name AS current_practice_name
    FROM {{ ref('dim_person_historical_practice') }}
    WHERE is_current_registration = TRUE
    -- Deduplicate: Choose the most recent current registration if multiple exist
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id 
        ORDER BY registration_start_date DESC, practice_id DESC
    ) = 1
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
