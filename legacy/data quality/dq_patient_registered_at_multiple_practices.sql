CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DQ_PATIENT_REGISTERED_AT_MULTIPLE_PRACTICES(
    PERSON_ID VARCHAR COMMENT 'Unique identifier for a person',
    SK_PATIENT_ID VARCHAR COMMENT 'Surrogate key for the patient',
    TOTAL_REGISTRATIONS NUMBER COMMENT 'Total number of practice registrations for this person',
    HAS_OVERLAPPING_REGISTRATIONS BOOLEAN COMMENT 'Flag indicating if person has any overlapping practice registration periods',
    HAS_REGISTRATION_GAPS BOOLEAN COMMENT 'Flag indicating if person has gaps between practice registrations',
    EARLIEST_REGISTRATION_DATE TIMESTAMP_NTZ COMMENT 'Date of person\'s first practice registration',
    LATEST_REGISTRATION_DATE TIMESTAMP_NTZ COMMENT 'Date of person\'s most recent practice registration',
    CURRENT_REGISTRATION_COUNT NUMBER COMMENT 'Number of apparently current (non-ended) registrations',
    OVERLAPPING_PERIODS_COUNT NUMBER COMMENT 'Number of registration periods that overlap with other periods',
    LONGEST_GAP_DAYS NUMBER COMMENT 'Longest gap in days between consecutive practice registrations',
    DQ_ISSUES ARRAY COMMENT 'Array of specific data quality issues identified'
)
COMMENT = 'Data quality table identifying potential data quality issues in practice registration data using Episode of Care, focusing on multiple registrations, overlaps, and gaps.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH registration_periods AS (
    -- Get all registration periods from Episode of Care with lead/lag analysis
    SELECT
        pp."person_id" AS PERSON_ID,
        p."sk_patient_id" AS SK_PATIENT_ID,
        eoc."episode_of_care_start_date" AS REGISTRATION_START_DATE,
        eoc."episode_of_care_end_date" AS REGISTRATION_END_DATE,
        -- Get next registration's start date for gap analysis
        LEAD(eoc."episode_of_care_start_date") OVER (
            PARTITION BY pp."person_id"
            ORDER BY eoc."episode_of_care_start_date"
        ) AS next_registration_start,
        -- Get previous registration's end date for overlap analysis
        LAG(eoc."episode_of_care_end_date") OVER (
            PARTITION BY pp."person_id"
            ORDER BY eoc."episode_of_care_start_date"
        ) AS prev_registration_end,
        -- Count registrations per person
        COUNT(*) OVER (PARTITION BY pp."person_id") AS total_registrations,
        -- Count current (non-ended) registrations
        SUM(CASE WHEN eoc."episode_of_care_end_date" IS NULL THEN 1 ELSE 0 END)
            OVER (PARTITION BY pp."person_id") AS current_registration_count
    FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
        ON pp."patient_id" = p."id"
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.EPISODE_OF_CARE eoc
        ON pp."person_id" = eoc."person_id"
    WHERE eoc."person_id" IS NOT NULL
        AND eoc."organisation_id" IS NOT NULL
        AND eoc."episode_of_care_start_date" IS NOT NULL
        -- Add episode type filter if needed to identify registration episodes
        -- AND eoc."episode_type_raw_concept_id" = 'REGISTRATION_TYPE_ID'
),
registration_analysis AS (
    -- Analyse each registration period for overlaps and gaps
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        total_registrations,
        current_registration_count,
        MIN(REGISTRATION_START_DATE) OVER (PARTITION BY PERSON_ID) AS earliest_registration,
        MAX(REGISTRATION_START_DATE) OVER (PARTITION BY PERSON_ID) AS latest_registration,
        -- Check for overlaps with previous registration
        CASE WHEN
            prev_registration_end IS NOT NULL
            AND REGISTRATION_START_DATE <= prev_registration_end
        THEN 1 ELSE 0 END AS has_overlap,
        -- Check for gaps with next registration
        CASE WHEN
            next_registration_start IS NOT NULL
            AND REGISTRATION_END_DATE IS NOT NULL
            AND DATEDIFF(day, REGISTRATION_END_DATE, next_registration_start) > 0
        THEN 1 ELSE 0 END AS has_gap,
        -- Calculate gap duration if exists
        CASE WHEN
            next_registration_start IS NOT NULL
            AND REGISTRATION_END_DATE IS NOT NULL
            AND DATEDIFF(day, REGISTRATION_END_DATE, next_registration_start) > 0
        THEN DATEDIFF(day, REGISTRATION_END_DATE, next_registration_start)
        ELSE 0 END AS gap_days
    FROM registration_periods
),
person_summary AS (
    -- Summarise issues at person level
    SELECT
        PERSON_ID,
        SK_PATIENT_ID,
        total_registrations,
        current_registration_count,
        earliest_registration AS EARLIEST_REGISTRATION_DATE,
        latest_registration AS LATEST_REGISTRATION_DATE,
        SUM(has_overlap) AS overlapping_periods_count,
        MAX(has_overlap) = 1 AS has_overlapping_registrations,
        MAX(has_gap) = 1 AS has_registration_gaps,
        MAX(gap_days) AS longest_gap_days,
        -- Build array of specific issues
        ARRAY_AGG(
            CASE
                WHEN has_overlap = 1 THEN 'Overlapping Registration Periods'
                WHEN has_gap = 1 THEN 'Gap Between Registrations'
                WHEN current_registration_count > 1 THEN 'Multiple Current Registrations'
                ELSE NULL
            END
        ) WITHIN GROUP (ORDER BY CASE
                WHEN has_overlap = 1 THEN 1
                WHEN has_gap = 1 THEN 2
                WHEN current_registration_count > 1 THEN 3
                ELSE 4
            END) AS all_issues
    FROM registration_analysis
    GROUP BY
        PERSON_ID,
        SK_PATIENT_ID,
        total_registrations,
        current_registration_count,
        earliest_registration,
        latest_registration
)
SELECT
    PERSON_ID,
    SK_PATIENT_ID,
    total_registrations AS TOTAL_REGISTRATIONS,
    has_overlapping_registrations AS HAS_OVERLAPPING_REGISTRATIONS,
    has_registration_gaps AS HAS_REGISTRATION_GAPS,
    EARLIEST_REGISTRATION_DATE,
    LATEST_REGISTRATION_DATE,
    current_registration_count AS CURRENT_REGISTRATION_COUNT,
    overlapping_periods_count AS OVERLAPPING_PERIODS_COUNT,
    longest_gap_days AS LONGEST_GAP_DAYS,
    ARRAY_COMPACT(all_issues) AS DQ_ISSUES
FROM person_summary
-- Only include records with potential issues
WHERE
    total_registrations > 1
    OR current_registration_count > 1
    OR has_overlapping_registrations
    OR has_registration_gaps
ORDER BY
    CASE
        WHEN current_registration_count > 1 THEN 1
        WHEN has_overlapping_registrations THEN 2
        WHEN has_registration_gaps THEN 3
        ELSE 4
    END,
    total_registrations DESC;
