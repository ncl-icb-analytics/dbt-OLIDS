{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'active', 'status'],
        cluster_by=['person_id'])
}}

/*
Person Active Status Dimension Table

Unified table combining active and inactive patient logic.
Provides comprehensive patient status for all persons including activity flags,
reasons for inactivity, and registration details.

Key Features:

• Combines logic from dim_person_active_patients and dim_person_inactive_patients

• Single source of truth for person activity status

• Includes both active (TRUE) and inactive (FALSE) persons

• Tracks reasons for inactivity (deceased, practice closed, etc.)

• Excludes dummy patients as they are not real persons

• Uses proper registration data from episode_of_care via int_patient_registrations
*/

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
        ipr.practitioner_id
    FROM {{ ref('int_patient_registrations') }} AS ipr
    WHERE ipr.is_current_registration = TRUE
),

latest_patient_record_per_person AS (
    -- Get the latest patient record for each person with comprehensive status logic
    SELECT
        ap.person_id,
        p.sk_patient_id,
        dp.patient_ids,
        
        -- Patient flags
        p.is_dummy_patient,
        p.is_confidential,
        p.is_spine_sensitive,
        
        -- Demographics
        p.birth_year,
        p.birth_month,
        p.death_year,
        p.death_month,
        p.death_year IS NOT NULL AS is_deceased,
        
        -- Current registration details (from active patient logic)
        cr.current_practice_id,
        cr.current_practice_code,
        cr.current_practice_name,
        cr.current_registration_start,
        cr.current_registration_duration,
        cr.total_registrations_count,
        cr.has_changed_practice,
        cr.practitioner_id,
        
        -- Historical practice details (from inactive patient logic)
        php.practice_id AS registered_practice_id,
        php.practice_name AS historical_practice_name,
        php.practice_type_code,
        php.practice_type_desc,
        php.practice_postcode,
        php.practice_parent_org_id,
        php.practice_open_date,
        php.practice_close_date,
        php.practice_is_obsolete,
        
        -- Record metadata
        p.record_owner_organisation_code AS record_owner_org_code,
        p.lds_datetime_data_acquired AS latest_record_date,
        
        -- Comprehensive activity status logic
        CASE
            WHEN p.death_year IS NOT NULL THEN FALSE -- Deceased
            WHEN p.is_dummy_patient THEN FALSE -- Dummy patient
            WHEN cr.person_id IS NULL THEN FALSE -- No current registration
            WHEN php.practice_close_date IS NOT NULL THEN FALSE -- Practice closed
            WHEN php.practice_is_obsolete THEN FALSE -- Practice obsolete
            ELSE TRUE
        END AS is_active,
        
        -- Reason for inactivity
        CASE
            WHEN p.death_year IS NOT NULL THEN 'Deceased'
            WHEN p.is_dummy_patient THEN 'Dummy Patient'
            WHEN cr.person_id IS NULL THEN 'No Current Registration'
            WHEN php.practice_close_date IS NOT NULL THEN 'Practice Closed'
            WHEN php.practice_is_obsolete THEN 'Practice Obsolete'
            ELSE NULL
        END AS inactive_reason,
        
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
        ON COALESCE(cr.patient_id, dp.sk_patient_ids[0]) = p.id -- Use current registration patient_id or fallback to first patient ID
    LEFT JOIN {{ ref('dim_person_historical_practice') }} AS php
        ON ap.person_id = php.person_id
        AND php.is_current_registration = TRUE
)

-- Select only the latest record per person (both active and inactive)
SELECT
    person_id,
    sk_patient_id,
    patient_ids,
    
    -- Status flags
    is_active,
    is_deceased,
    is_dummy_patient,
    is_confidential,
    is_spine_sensitive,
    inactive_reason,
    
    -- Demographics
    birth_year,
    birth_month,
    death_year,
    death_month,
    
    -- Current registration details (for active patients)
    current_practice_id,
    current_practice_code,
    current_practice_name,
    current_registration_start,
    current_registration_duration,
    total_registrations_count,
    has_changed_practice,
    practitioner_id,
    
    -- Historical practice details (for inactive patients)
    registered_practice_id,
    historical_practice_name,
    practice_type_code,
    practice_type_desc,
    practice_postcode,
    practice_parent_org_id,
    practice_open_date,
    practice_close_date,
    practice_is_obsolete,
    
    -- Record metadata
    record_owner_org_code,
    latest_record_date

FROM latest_patient_record_per_person
WHERE record_rank = 1
  AND is_dummy_patient = FALSE -- Exclude dummy patients from both active and inactive