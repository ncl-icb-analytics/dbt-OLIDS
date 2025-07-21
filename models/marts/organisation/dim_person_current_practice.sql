{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'practice', 'current'],
        cluster_by=['person_id'])
}}

-- Person Current Practice Dimension Table
-- Simple view of current practice registrations from historical practice table
-- Ensures exactly one row per person with valid person_id

WITH current_registrations AS (
    SELECT
        person_id,
        sk_patient_id,
        practice_id,
        practice_code,
        practice_name,
        practice_type_code,
        practice_type_desc,
        practice_postcode,
        practice_parent_org_id,
        practice_open_date,
        practice_close_date,
        practice_is_obsolete,
        registration_start_date,
        registration_end_date,
        registration_duration_days,
        registration_sequence,
        total_registrations_count,
        has_changed_practice,
        age_at_registration_start,
        practitioner_id,
        -- Add row number to handle potential duplicates
        ROW_NUMBER() OVER (
            PARTITION BY person_id 
            ORDER BY registration_start_date DESC, practice_id
        ) AS rn
    FROM {{ ref('dim_person_historical_practice') }}
    WHERE is_current_registration = TRUE  -- person_id filtering handled upstream
)

SELECT
    -- Core identifiers
    person_id,
    sk_patient_id,
    practice_id,
    practice_code,
    practice_name,
    
    -- Practice details
    practice_type_code,
    practice_type_desc,
    practice_postcode,
    practice_parent_org_id,
    practice_open_date,
    practice_close_date,
    practice_is_obsolete,
    
    -- Registration details
    registration_start_date,
    registration_end_date,
    registration_duration_days,
    registration_sequence,
    total_registrations_count,
    has_changed_practice,
    
    -- Additional useful fields
    age_at_registration_start,
    practitioner_id
FROM current_registrations
WHERE rn = 1  -- Ensure only one row per person
