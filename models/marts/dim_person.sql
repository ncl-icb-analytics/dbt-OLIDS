{{
    config(
        materialized='table',
        tags=['dimension', 'person'],
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Simplified person dimension providing aggregated patient and practice relationships. Uses arrays to efficiently store multiple patient IDs and practice associations per person.'"
        ]
    )
}}

-- Person Dimension Table - Simplified
-- Aggregates person-to-patient relationships and practice associations
-- Uses arrays to store multiple patient IDs and practice information per person
-- EXACTLY MATCHES LEGACY: DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON

WITH person_patients AS (
    -- Get all patient relationships for each person
    SELECT 
        pp.person_id,
        ARRAY_AGG(DISTINCT p.sk_patient_id) AS sk_patient_ids,
        ARRAY_AGG(DISTINCT p.id) AS patient_ids,
        COUNT(DISTINCT p.id) AS total_patients
    FROM {{ ref('stg_olids_patient_person') }} pp
    JOIN {{ ref('stg_olids_patient') }} p 
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
    WHERE is_current_practice = TRUE
)

-- Final aggregation
SELECT 
    pp.person_id,
    pp.sk_patient_ids,
    pp.patient_ids,
    COALESCE(pr.practice_ids, ARRAY_CONSTRUCT()) AS practice_ids,
    COALESCE(pr.practice_codes, ARRAY_CONSTRUCT()) AS practice_codes,
    COALESCE(pr.practice_names, ARRAY_CONSTRUCT()) AS practice_names,
    cp.current_practice_id,
    cp.current_practice_code,
    cp.current_practice_name,
    pp.total_patients,
    COALESCE(pr.total_practices, 0) AS total_practices
FROM person_patients pp
LEFT JOIN person_practices pr 
    ON pp.person_id = pr.person_id
LEFT JOIN current_practices cp 
    ON pp.person_id = cp.person_id 