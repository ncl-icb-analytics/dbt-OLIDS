{{
    config(
        materialized='table',
        tags=['intermediate', 'registration', 'patient', 'practice'],
        cluster_by=['person_id', 'registration_start_date'])
}}

-- Patient Registrations - Clean registration periods from PATIENT_REGISTERED_PRACTITIONER_IN_ROLE
-- Processes raw PATIENT_REGISTERED_PRACTITIONER_IN_ROLE into proper registration periods
-- Uses PATIENT_PERSON bridge table to get canonical person_id (not direct person_id field)
-- Handles overlapping registrations, active registrations, and historical periods
-- Forms the foundation for patient-practice relationship analysis
-- Uses the correct registration criteria for active patient determination

WITH patient_to_person AS (
    -- Get canonical person_id for each patient_id via deduplicated PATIENT_PERSON bridge table
    -- int_patient_person_unique already handles deduplication, so no ROW_NUMBER needed
    SELECT 
        pp.patient_id,
        pp.person_id
    FROM {{ ref('int_patient_person_unique') }} AS pp
),

raw_registrations AS (
    -- Get all registration episodes from PATIENT_REGISTERED_PRACTITIONER_IN_ROLE
    -- Use patient_id to get canonical person_id via PATIENT_PERSON bridge
    SELECT
        prpr.id AS registration_record_id,
        prpr.patient_id,
        ptp.person_id,  -- Use person_id from bridge table instead of direct field
        prpr.organisation_id,
        prpr.start_date AS registration_start_date,
        prpr.end_date AS registration_end_date,
        prpr.practitioner_id,
        prpr.episode_of_care_id,
        -- Get practice details
        o.name AS practice_name,
        o.organisation_code AS practice_ods_code,
        -- Get patient details
        p.sk_patient_id
    FROM {{ ref('stg_olids_patient_registered_practitioner_in_role') }} AS prpr
    INNER JOIN patient_to_person AS ptp
        ON prpr.patient_id = ptp.patient_id
    LEFT JOIN {{ ref('stg_olids_organisation') }} AS o
        ON prpr.organisation_id = o.id
    LEFT JOIN {{ ref('stg_olids_patient') }} AS p
        ON prpr.patient_id = p.id
    WHERE prpr.start_date IS NOT NULL
        AND prpr.patient_id IS NOT NULL  -- Filter out records without patient_id
),

cleaned_registrations AS (
    -- Clean and validate registration periods using new registration criteria
    SELECT
        rr.*,
        -- Determine if registration is currently active using the new criteria:
        -- Active if: end_date IS NULL OR end_date > CURRENT_DATE() OR end_date < start_date
        (
            rr.registration_end_date IS NULL 
            OR rr.registration_end_date > CURRENT_DATE()
            OR rr.registration_end_date < rr.registration_start_date
        ) AS is_current_registration,

        -- Calculate registration duration (only for completed registrations with valid end dates)
        CASE
            WHEN rr.registration_end_date IS NOT NULL
                AND rr.registration_end_date >= rr.registration_start_date
                THEN
                    DATEDIFF(
                        'day',
                        rr.registration_start_date,
                        rr.registration_end_date
                    )
        END AS registration_duration_days,

        -- Effective end date for analysis (NULL for active registrations)
        CASE
            WHEN (
                rr.registration_end_date IS NULL 
                OR rr.registration_end_date > CURRENT_DATE()
                OR rr.registration_end_date < rr.registration_start_date
            ) THEN NULL
            ELSE rr.registration_end_date
        END AS effective_end_date,

        -- Registration period classification
        CASE
            WHEN (
                rr.registration_end_date IS NULL 
                OR rr.registration_end_date > CURRENT_DATE()
                OR rr.registration_end_date < rr.registration_start_date
            ) THEN 'Active'
            ELSE 'Historical'
        END AS registration_status

    FROM raw_registrations AS rr
),

person_registration_sequences AS (
    -- Add sequence information for each person's registrations
    SELECT
        cr.*,
        -- Number registrations chronologically per person
        ROW_NUMBER() OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_date, cr.registration_record_id
        ) AS registration_sequence,

        -- Identify latest registration per person
        ROW_NUMBER() OVER (
            PARTITION BY cr.person_id
            ORDER BY
                cr.registration_start_date DESC, cr.registration_record_id DESC
        ) = 1 AS is_latest_registration,

        -- Count total registrations per person
        COUNT(*) OVER (PARTITION BY cr.person_id) AS total_registrations_count,

        -- Get next registration start date (for gap analysis)
        LEAD(cr.registration_start_date) OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_date, cr.registration_record_id
        ) AS next_registration_start,

        -- Get previous registration end date
        LAG(cr.effective_end_date) OVER (
            PARTITION BY cr.person_id
            ORDER BY cr.registration_start_date, cr.registration_record_id
        ) AS previous_registration_end

    FROM cleaned_registrations AS cr
)

-- Final selection with complete registration analysis
SELECT
    -- Core identifiers
    prs.registration_record_id,
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
    prs.episode_of_care_id

FROM person_registration_sequences AS prs
ORDER BY prs.person_id, prs.registration_start_date
