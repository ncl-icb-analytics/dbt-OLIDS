{{
    config(
        materialized='table',
        tags=['intermediate', 'registration', 'patient', 'practice', 'episode_of_care'],
        cluster_by=['person_id', 'registration_start_date'])
}}

-- Alternative Patient Registrations - Uses EPISODE_OF_CARE linked to PATIENT_REGISTERED_PRACTITIONER_IN_ROLE
-- Gets practitioner/organisation details from PATIENT_REGISTERED_PRACTITIONER_IN_ROLE via episode_of_care_id
-- Uses episode temporal periods for proper historical registration tracking
-- Uses PATIENT_PERSON bridge table to get canonical person_id

WITH patient_to_person AS (
    -- Get canonical person_id for each patient_id via deduplicated PATIENT_PERSON bridge table
    SELECT 
        pp.patient_id,
        pp.person_id
    FROM {{ ref('int_patient_person_unique') }} AS pp
),

raw_episodes AS (
    -- Get episode of care records linked to practitioner registrations
    -- Use practitioner role for organisation/practitioner details, episode for temporal periods
    SELECT
        eoc.id AS episode_record_id,
        eoc.patient_id,
        ptp.person_id,  -- Use person_id from bridge table instead of direct field
        prpr.organisation_id,
        eoc.episode_of_care_start_date AS registration_start_date,
        eoc.episode_of_care_end_date AS registration_end_date,
        prpr.practitioner_id,
        -- Get practice details using organisation_id from practitioner role
        o.name AS practice_name,
        o.organisation_code AS practice_ods_code,
        -- Get patient details
        p.sk_patient_id
    FROM {{ ref('stg_olids_episode_of_care') }} AS eoc
    INNER JOIN patient_to_person AS ptp
        ON eoc.patient_id = ptp.patient_id
    INNER JOIN {{ ref('stg_olids_patient_registered_practitioner_in_role') }} AS prpr
        ON eoc.id = prpr.episode_of_care_id
    LEFT JOIN {{ ref('stg_olids_organisation') }} AS o
        ON prpr.organisation_id = o.id
    LEFT JOIN {{ ref('stg_olids_patient') }} AS p
        ON eoc.patient_id = p.id
    WHERE eoc.episode_of_care_start_date IS NOT NULL
        AND eoc.patient_id IS NOT NULL  -- Filter out records without patient_id
        AND prpr.organisation_id IS NOT NULL  -- Filter out records without organisation
    -- FIX: Deduplicate identical episode records from source data
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY 
            eoc.patient_id,
            prpr.organisation_id,
            eoc.episode_of_care_start_date,
            eoc.episode_of_care_end_date,
            prpr.practitioner_id
        ORDER BY eoc.id  -- Use ID as tie-breaker for deterministic results
    ) = 1
),

cleaned_registrations AS (
    -- Clean and validate registration periods using same registration criteria as main table
    SELECT
        re.*,
        -- Determine if registration is currently active using the same criteria:
        -- Active if: end_date IS NULL OR end_date > CURRENT_DATE() OR end_date < start_date
        (
            re.registration_end_date IS NULL 
            OR re.registration_end_date > CURRENT_DATE()
            OR re.registration_end_date < re.registration_start_date
        ) AS is_current_registration,

        -- Calculate registration duration (only for completed registrations with valid end dates)
        CASE
            WHEN re.registration_end_date IS NOT NULL
                AND re.registration_end_date >= re.registration_start_date
                THEN
                    DATEDIFF(
                        'day',
                        re.registration_start_date,
                        re.registration_end_date
                    )
        END AS registration_duration_days,

        -- Effective end date for analysis (NULL for active registrations)
        CASE
            WHEN (
                re.registration_end_date IS NULL 
                OR re.registration_end_date > CURRENT_DATE()
                OR re.registration_end_date < re.registration_start_date
            ) THEN NULL
            ELSE re.registration_end_date
        END AS effective_end_date,

        -- Registration period classification
        CASE
            WHEN (
                re.registration_end_date IS NULL 
                OR re.registration_end_date > CURRENT_DATE()
                OR re.registration_end_date < re.registration_start_date
            ) THEN 'Active'
            ELSE 'Historical'
        END AS registration_status

    FROM raw_episodes AS re
),

person_registration_sequences AS (
    -- Add sequence information for each person's registrations
    SELECT
        cr.*,
        -- Number registrations chronologically per person
        ROW_NUMBER() OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_date, cr.episode_record_id
        ) AS registration_sequence,

        -- Identify latest registration per person
        ROW_NUMBER() OVER (
            PARTITION BY cr.person_id
            ORDER BY
                cr.registration_start_date DESC, cr.episode_record_id DESC
        ) = 1 AS is_latest_registration,

        -- Count total registrations per person
        COUNT(*) OVER (PARTITION BY cr.person_id) AS total_registrations_count,

        -- Get next registration start date (for gap analysis)
        LEAD(cr.registration_start_date) OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_date, cr.episode_record_id
        ) AS next_registration_start,

        -- Get previous registration end date
        LAG(cr.effective_end_date) OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_date, cr.episode_record_id
        ) AS previous_registration_end

    FROM cleaned_registrations AS cr
)

-- Final selection with complete registration analysis
SELECT
    -- Core identifiers
    prs.episode_record_id AS registration_record_id,
    prs.person_id,
    prs.patient_id,
    prs.sk_patient_id,
    prs.organisation_id,
    prs.practice_name,
    prs.practice_ods_code,

    -- Registration period details
    prs.registration_start_date,
    prs.registration_end_date,
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
            AND prs.registration_start_date
            > DATEADD('day', 1, prs.previous_registration_end)
            THEN
                DATEDIFF(
                    'day',
                    prs.previous_registration_end,
                    prs.registration_start_date
                )
    END AS gap_since_previous_registration_days,

    -- Registration metadata
    prs.practitioner_id,
    prs.episode_record_id AS episode_of_care_id

FROM person_registration_sequences AS prs
ORDER BY prs.person_id, prs.registration_start_date