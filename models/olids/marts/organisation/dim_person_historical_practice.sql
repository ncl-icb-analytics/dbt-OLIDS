{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'practice', 'historical'],
        cluster_by=['person_id', 'registration_start_date'])
}}

-- Person Historical Practice Dimension Table
-- Now uses proper registration data from episode_of_care via int_patient_registrations
-- Tracks all practice registrations (current and historical) for each person

WITH enriched_registrations AS (
    -- Get all registrations with additional practice details
    SELECT
        ipr.person_id,
        ipr.sk_patient_id,
        ipr.organisation_id AS practice_id,
        ipr.practice_name,
        ipr.practice_ods_code AS practice_code,
        ipr.registration_start_date,
        ipr.registration_end_date,
        ipr.effective_end_date,
        ipr.registration_duration_days,
        ipr.registration_status,
        ipr.is_current_registration,
        ipr.is_latest_registration,
        ipr.registration_sequence,
        ipr.total_registrations_count,
        ipr.gap_since_previous_registration_days,
        ipr.has_changed_practice,
        ipr.practitioner_id,
        -- Add organisation details
        o.type_code AS practice_type_code,
        o.type_desc AS practice_type_desc,
        o.postcode AS practice_postcode,
        o.parent_organisation_id AS practice_parent_org_id,
        o.open_date AS practice_open_date,
        o.close_date AS practice_close_date,
        o.is_obsolete AS practice_is_obsolete,
        -- Additional registration analysis
        CASE
            WHEN ipr.registration_end_date IS NULL THEN 'Open Registration'
            ELSE 'Closed Registration'
        END AS registration_type,
        -- Calculate age at registration start (approximate)
        CASE
            WHEN p.birth_year IS NOT NULL
                THEN YEAR(ipr.registration_start_date) - p.birth_year
        END AS age_at_registration_start
    FROM {{ ref('int_patient_registrations') }} AS ipr
    LEFT JOIN {{ ref('stg_olids_organisation') }} AS o
        ON ipr.organisation_id = o.id
    LEFT JOIN {{ ref('stg_olids_patient') }} AS p
        ON ipr.patient_id = p.id
    -- person_id filtering handled upstream in int_patient_registrations
),

registration_transitions AS (
    -- Add information about transitions between practices
    SELECT
        er.*,
        -- Get previous practice details
        LAG(er.practice_id) OVER (
            PARTITION BY er.person_id
            ORDER BY er.registration_start_date
        ) AS previous_practice_id,
        LAG(er.practice_name) OVER (
            PARTITION BY er.person_id
            ORDER BY er.registration_start_date
        ) AS previous_practice_name,
        LAG(er.registration_end_date) OVER (
            PARTITION BY er.person_id
            ORDER BY er.registration_start_date
        ) AS previous_registration_end_date,
        -- Get next practice details
        LEAD(er.practice_id) OVER (
            PARTITION BY er.person_id
            ORDER BY er.registration_start_date
        ) AS next_practice_id,
        LEAD(er.practice_name) OVER (
            PARTITION BY er.person_id
            ORDER BY er.registration_start_date
        ) AS next_practice_name,
        LEAD(er.registration_start_date) OVER (
            PARTITION BY er.person_id
            ORDER BY er.registration_start_date
        ) AS next_registration_start_date
    FROM enriched_registrations AS er
)

-- Final selection with all registration history
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
    effective_end_date,
    registration_duration_days,
    registration_status,
    registration_type,
    is_current_registration,
    is_latest_registration,
    registration_sequence,
    total_registrations_count,
    gap_since_previous_registration_days,
    has_changed_practice,
    age_at_registration_start,
    practitioner_id,
    -- Practice transition information
    previous_practice_id,
    previous_practice_name,
    previous_registration_end_date,
    next_practice_id,
    next_practice_name,
    next_registration_start_date,
    -- Additional computed fields
    registration_sequence = 1 AS is_first_registration,
    registration_sequence = total_registrations_count AS is_last_registration
FROM registration_transitions
ORDER BY person_id, registration_start_date
