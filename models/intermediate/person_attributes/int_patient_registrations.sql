{{
    config(
        materialized='table',
        tags=['intermediate', 'registration', 'patient', 'practice'],
        cluster_by=['person_id', 'registration_start_date'])
}}

-- Patient Registrations - Clean registration periods from episode_of_care
-- Processes raw episode_of_care into proper registration periods
-- Handles overlapping registrations, active registrations, and historical periods
-- Forms the foundation for patient-practice relationship analysis
-- Designed for incremental loading - avoids CURRENT_DATE() calculations

WITH raw_registrations AS (
    -- Get all registration episodes from episode_of_care
    SELECT
        eoc.id AS episode_of_care_id,
        eoc.patient_id,
        eoc.person_id,
        eoc.organisation_id,
        eoc.episode_of_care_start_date,
        eoc.episode_of_care_end_date,
        eoc.episode_type_raw_concept_id,
        eoc.episode_status_raw_concept_id,
        eoc.care_manager_practitioner_id,
        -- Get practice details
        o.name AS practice_name,
        o.organisation_code AS practice_ods_code,
        -- Get patient details
        p.sk_patient_id
    FROM {{ ref('stg_olids_episode_of_care') }} AS eoc
    LEFT JOIN {{ ref('stg_olids_organisation') }} AS o
        ON eoc.organisation_id = o.id
    LEFT JOIN {{ ref('stg_olids_patient') }} AS p
        ON eoc.patient_id = p.id
    WHERE eoc.episode_of_care_start_date IS NOT NULL
        AND eoc.person_id IS NOT NULL  -- Filter out records without person_id for person-based analysis
),

cleaned_registrations AS (
    -- Clean and validate registration periods
    SELECT
        rr.*,
        -- Determine if registration is currently active (based on end date presence)
        rr.episode_of_care_end_date IS NULL AS is_current_registration,

        -- Calculate registration duration (only for completed registrations)
        CASE
            WHEN rr.episode_of_care_end_date IS NOT NULL
                THEN
                    DATEDIFF(
                        'day',
                        rr.episode_of_care_start_date,
                        rr.episode_of_care_end_date
                    )
        END AS registration_duration_days,

        -- Effective end date for analysis (NULL for active registrations)
        rr.episode_of_care_end_date AS effective_end_date,

        -- Registration period classification
        CASE
            WHEN rr.episode_of_care_end_date IS NULL THEN 'Active'
            ELSE 'Historical'
        END AS registration_status

    FROM raw_registrations AS rr
    -- Remove future start date filter to support incremental loading
),

person_registration_sequences AS (
    -- Add sequence information for each person's registrations
    SELECT
        cr.*,
        -- Number registrations chronologically per person
        ROW_NUMBER() OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.episode_of_care_start_date, cr.episode_of_care_id
        ) AS registration_sequence,

        -- Identify latest registration per person
        ROW_NUMBER() OVER (
            PARTITION BY cr.person_id
            ORDER BY
                cr.episode_of_care_start_date DESC, cr.episode_of_care_id DESC
        ) = 1 AS is_latest_registration,

        -- Count total registrations per person
        COUNT(*) OVER (PARTITION BY cr.person_id) AS total_registrations_count,

        -- Get next registration start date (for gap analysis)
        LEAD(cr.episode_of_care_start_date) OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.episode_of_care_start_date, cr.episode_of_care_id
        ) AS next_registration_start,

        -- Get previous registration end date
        LAG(cr.episode_of_care_end_date) OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.episode_of_care_start_date, cr.episode_of_care_id
        ) AS previous_registration_end

    FROM cleaned_registrations AS cr
)

-- Final selection with complete registration analysis
SELECT
    -- Core identifiers
    prs.episode_of_care_id,
    prs.person_id,
    prs.patient_id,
    prs.sk_patient_id,
    prs.organisation_id,
    prs.practice_name,
    prs.practice_ods_code,

    -- Registration period details
    prs.episode_of_care_start_date AS registration_start_date,
    prs.episode_of_care_end_date AS registration_end_date,
    prs.effective_end_date,
    prs.registration_duration_days,
    prs.registration_status,

    -- Registration flags
    prs.is_current_registration,
    prs.is_latest_registration,

    -- Sequence information
    prs.registration_sequence,
    prs.total_registrations_count,
    prs.registration_sequence > 1 AS has_changed_practice,

    -- Gap analysis
    CASE
        WHEN
            prs.previous_registration_end IS NOT NULL
            AND prs.episode_of_care_start_date
            > DATEADD('day', 1, prs.previous_registration_end)
            THEN
                DATEDIFF(
                    'day',
                    prs.previous_registration_end,
                    prs.episode_of_care_start_date
                )
    END AS gap_since_previous_registration_days,

    -- Episode metadata
    prs.episode_type_raw_concept_id,
    prs.episode_status_raw_concept_id,
    prs.care_manager_practitioner_id

FROM person_registration_sequences AS prs
ORDER BY prs.person_id, prs.episode_of_care_start_date
