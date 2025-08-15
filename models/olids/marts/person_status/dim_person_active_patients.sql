{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'active'],
        cluster_by=['person_id'])
}}

-- Person Active Patients Dimension Table
-- Now uses proper registration data from episode_of_care via int_patient_registrations
-- Filters out deceased patients and dummy patients
-- Links PATIENT_PERSON, PATIENT, PERSON, and proper registration data

WITH all_persons AS (
    SELECT person_id
    FROM {{ ref('dim_person') }}
),

current_registrations AS (
    -- Get current registration details per person
    SELECT
        ipr.person_id,
        ipr.patient_id,
        ipr.organisation_id AS current_practice_id,
        ipr.practice_name AS current_practice_name,
        ipr.practice_ods_code AS current_practice_code,
        ipr.registration_start_date AS current_registration_start,
        ipr.registration_duration_days AS current_registration_duration,
        ipr.total_registrations_count,
        ipr.has_changed_practice,
        -- Additional registration metadata
        ipr.practitioner_id
    FROM {{ ref('int_patient_registrations') }} AS ipr
    WHERE ipr.is_current_registration = TRUE
),

latest_patient_record_per_person AS (
    -- Get the latest patient record for each person with registration details
    SELECT
        ap.person_id,
        p.sk_patient_id,
        dp.patient_ids,
        -- Determine if patient is active based on various criteria
        p.is_dummy_patient,
        p.is_confidential,
        p.is_spine_sensitive,
        p.birth_year,
        p.birth_month,
        p.death_year,
        p.death_month,
        cr.current_practice_id,
        cr.current_practice_code,
        -- Registration details from proper episode_of_care data
        cr.current_practice_name,
        cr.current_registration_start,
        cr.current_registration_duration,
        cr.total_registrations_count,
        cr.has_changed_practice,
        cr.practitioner_id,
        p.record_owner_organisation_code AS record_owner_org_code,
        p.lds_datetime_data_acquired AS latest_record_date,
        CASE
            WHEN p.death_year IS NOT NULL THEN FALSE -- Deceased
            WHEN p.is_dummy_patient THEN FALSE -- Dummy patient
            WHEN cr.person_id IS NULL THEN FALSE -- No current registration
            ELSE TRUE
        END AS is_active,
        p.death_year IS NOT NULL AS is_deceased,
        -- Rank to get the latest record
        ROW_NUMBER() OVER (
            PARTITION BY ap.person_id
            ORDER BY
                p.lds_datetime_data_acquired DESC,
                p.id DESC
        ) AS record_rank
    FROM all_persons AS ap
    LEFT JOIN {{ ref('dim_person') }} AS dp
        ON ap.person_id = dp.person_id
    LEFT JOIN current_registrations AS cr
        ON ap.person_id = cr.person_id
    LEFT JOIN {{ ref('stg_olids_patient') }} AS p
        ON cr.patient_id = p.id
)

-- Select only the latest record per person and only active patients
SELECT
    person_id,
    sk_patient_id,
    patient_ids,
    is_active,
    is_deceased,
    is_dummy_patient,
    is_confidential,
    is_spine_sensitive,
    birth_year,
    birth_month,
    death_year,
    death_month,
    -- Registration-based practice information
    current_practice_id,
    current_practice_code,
    current_practice_name,
    current_registration_start,
    current_registration_duration,
    total_registrations_count,
    has_changed_practice,
    practitioner_id,
    record_owner_org_code,
    latest_record_date
FROM latest_patient_record_per_person
WHERE
    record_rank = 1
    AND is_active = TRUE
