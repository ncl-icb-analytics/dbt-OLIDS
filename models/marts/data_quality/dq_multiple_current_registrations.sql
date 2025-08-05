{{
    config(
        materialized='table',
        tags=['data_quality', 'registration', 'person'],
        cluster_by=['person_id'])
}}

-- Multiple Current Registrations Data Quality Issues
-- Identifies persons who have multiple active registrations simultaneously
-- This indicates potential data quality issues with registration end dates
-- or business process issues with registration management

WITH persons_with_multiple_current AS (
    -- Identify all persons with more than one current registration
    SELECT 
        person_id,
        COUNT(*) as current_registration_count
    FROM {{ ref('dim_person_historical_practice') }}
    WHERE is_current_registration = TRUE
    GROUP BY person_id
    HAVING COUNT(*) > 1
),

registration_details AS (
    -- Get all current registration details for these persons
    SELECT 
        dhp.person_id,
        dhp.sk_patient_id,
        dhp.practice_id,
        dhp.practice_code,
        dhp.practice_name,
        dhp.practice_type_desc,
        dhp.practice_postcode,
        dhp.registration_start_date,
        dhp.registration_end_date,
        dhp.effective_end_date,
        dhp.registration_duration_days,
        dhp.registration_status,
        dhp.registration_type,
        dhp.is_current_registration,
        dhp.is_latest_registration,
        dhp.registration_sequence,
        dhp.total_registrations_count,
        dhp.practitioner_id,
        pwmc.current_registration_count,
        
        -- Classify why this registration is considered current
        CASE 
            WHEN dhp.registration_end_date IS NULL THEN 'NULL_END_DATE'
            WHEN dhp.registration_end_date > CURRENT_DATE() THEN 'FUTURE_END_DATE'
            WHEN dhp.registration_end_date < dhp.registration_start_date THEN 'END_BEFORE_START'
            ELSE 'OTHER'
        END AS current_registration_reason,
        
        -- Check if this is the same practice
        LAG(dhp.practice_id) OVER (
            PARTITION BY dhp.person_id 
            ORDER BY dhp.registration_start_date
        ) AS previous_practice_id,
        
        -- Check for overlapping periods
        LAG(dhp.registration_end_date) OVER (
            PARTITION BY dhp.person_id 
            ORDER BY dhp.registration_start_date
        ) AS previous_registration_end_date
        
    FROM {{ ref('dim_person_historical_practice') }} dhp
    INNER JOIN persons_with_multiple_current pwmc
        ON dhp.person_id = pwmc.person_id
    WHERE dhp.is_current_registration = TRUE
),

overlap_analysis AS (
    -- Add overlap detection flags
    SELECT 
        *,
        
        -- Flag if this registration overlaps with the previous one
        CASE 
            WHEN previous_registration_end_date IS NULL 
                AND previous_practice_id IS NOT NULL THEN TRUE
            WHEN previous_registration_end_date > registration_start_date THEN TRUE
            ELSE FALSE
        END AS overlaps_with_previous,
        
        -- Flag if registrations are at the same practice
        CASE 
            WHEN practice_id = previous_practice_id THEN TRUE
            ELSE FALSE
        END AS is_same_practice_as_previous,
        
        -- Rank registrations by start date for each person
        ROW_NUMBER() OVER (
            PARTITION BY person_id 
            ORDER BY registration_start_date DESC
        ) AS registration_recency_rank
        
    FROM registration_details
)

-- Final output with comprehensive DQ flags
SELECT 
    person_id,
    sk_patient_id,
    practice_id,
    practice_code,
    practice_name,
    practice_type_desc,
    practice_postcode,
    registration_start_date,
    registration_end_date,
    effective_end_date,
    registration_duration_days,
    registration_status,
    registration_type,
    is_latest_registration,
    registration_sequence,
    total_registrations_count,
    current_registration_count,
    registration_recency_rank,
    practitioner_id,
    
    -- DQ Issue Classification
    current_registration_reason,
    overlaps_with_previous,
    is_same_practice_as_previous,
    
    -- DQ Severity Flags
    CASE 
        WHEN current_registration_reason = 'NULL_END_DATE' THEN 'Missing End Date'
        WHEN current_registration_reason = 'FUTURE_END_DATE' THEN 'Future End Date'
        WHEN current_registration_reason = 'END_BEFORE_START' THEN 'End Before Start (Data Error)'
        ELSE 'Unknown Issue'
    END AS dq_issue_type,
    
    CASE 
        WHEN current_registration_reason = 'END_BEFORE_START' THEN 'HIGH'
        WHEN overlaps_with_previous AND NOT is_same_practice_as_previous THEN 'HIGH'
        WHEN current_registration_reason = 'NULL_END_DATE' THEN 'MEDIUM'
        WHEN current_registration_reason = 'FUTURE_END_DATE' THEN 'LOW'
        ELSE 'LOW'
    END AS dq_severity,
    
    -- Additional context flags
    CASE 
        WHEN current_registration_count > 2 THEN TRUE 
        ELSE FALSE 
    END AS has_more_than_two_current,
    
    CASE 
        WHEN overlaps_with_previous AND NOT is_same_practice_as_previous THEN TRUE
        ELSE FALSE
    END AS has_conflicting_practice_overlap

FROM overlap_analysis

ORDER BY 
    dq_severity DESC,
    person_id, 
    registration_start_date DESC