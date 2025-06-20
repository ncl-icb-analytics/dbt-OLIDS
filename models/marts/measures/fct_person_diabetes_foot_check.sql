{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['person_id'], 'unique': false},
            {'columns': ['person_id', 'foot_check_completed_in_last_12m'], 'unique': false}
        ]
    )
}}

SELECT 
    d.person_id,
    d.diabetes_type,
    
    -- Latest foot check details
    fc.clinical_effective_date AS latest_foot_check_date,
    
    -- Foot check completion logic (complex business rules)
    CASE
        WHEN fc.clinical_effective_date IS NOT NULL 
            AND DATEDIFF(month, fc.clinical_effective_date, CURRENT_DATE()) <= 12
            AND (
                fc.both_feet_checked 
                OR (fc.left_foot_checked AND (fc.right_foot_absent OR fc.right_foot_amputated))
                OR (fc.right_foot_checked AND (fc.left_foot_absent OR fc.left_foot_amputated))
            )
            AND NOT (fc.is_unsuitable OR fc.is_declined)
        THEN TRUE
        ELSE FALSE
    END AS foot_check_completed_in_last_12m,
    
    -- Permanent exemption (both feet absent/amputated)
    CASE
        WHEN (fc.left_foot_absent OR fc.left_foot_amputated)
            AND (fc.right_foot_absent OR fc.right_foot_amputated)
        THEN TRUE
        ELSE FALSE
    END AS is_permanently_exempt,
    
    -- Status of most recent check attempt
    CASE
        WHEN fc.clinical_effective_date IS NULL THEN 'Not Done'
        WHEN fc.is_declined THEN 'Declined'
        WHEN fc.is_unsuitable THEN 'Unsuitable'
        WHEN fc.both_feet_checked OR (fc.left_foot_checked AND fc.right_foot_checked) THEN 'Complete - Both Feet'
        WHEN fc.left_foot_checked AND (fc.right_foot_absent OR fc.right_foot_amputated) THEN 'Complete - Left Only (Right Missing)'
        WHEN fc.right_foot_checked AND (fc.left_foot_absent OR fc.left_foot_amputated) THEN 'Complete - Right Only (Left Missing)'
        WHEN fc.left_foot_checked THEN 'Partial - Left Only'
        WHEN fc.right_foot_checked THEN 'Partial - Right Only'
        ELSE 'Not Done'
    END AS latest_check_status,
    
    -- Detailed status including permanent exemptions
    CASE
        WHEN (fc.left_foot_absent OR fc.left_foot_amputated)
            AND (fc.right_foot_absent OR fc.right_foot_amputated)
        THEN 'Permanently Exempt - Both Feet Missing'
        WHEN fc.is_unsuitable THEN 'Not Appropriate - Unsuitable'
        WHEN fc.is_declined THEN 'Not Appropriate - Declined'
        WHEN fc.both_feet_checked THEN 'Complete - Both Feet'
        WHEN fc.left_foot_checked AND fc.right_foot_checked THEN 'Complete - Both Feet'
        WHEN fc.left_foot_checked AND (fc.right_foot_absent OR fc.right_foot_amputated) THEN 'Complete - Left Only (Right Missing)'
        WHEN fc.right_foot_checked AND (fc.left_foot_absent OR fc.left_foot_amputated) THEN 'Complete - Right Only (Left Missing)'
        WHEN fc.left_foot_checked THEN 'Partial - Left Only'
        WHEN fc.right_foot_checked THEN 'Partial - Right Only'
        WHEN fc.clinical_effective_date IS NULL THEN 'Not Done'
        ELSE 'Not Done'
    END AS foot_check_status,
    
    -- Individual foot status with risk levels
    CASE
        WHEN fc.left_foot_absent THEN 'Absent (Congenital)'
        WHEN fc.left_foot_amputated THEN 'Amputated'
        WHEN fc.left_foot_risk_level IS NOT NULL THEN fc.left_foot_risk_level || ' Risk'
        WHEN fc.left_foot_checked THEN 'Risk Level Not Recorded'
        ELSE 'Not Assessed'
    END AS left_foot_status,
    
    CASE
        WHEN fc.right_foot_absent THEN 'Absent (Congenital)'
        WHEN fc.right_foot_amputated THEN 'Amputated'
        WHEN fc.right_foot_risk_level IS NOT NULL THEN fc.right_foot_risk_level || ' Risk'
        WHEN fc.right_foot_checked THEN 'Risk Level Not Recorded'
        ELSE 'Not Assessed'
    END AS right_foot_status,
    
    -- Townson scale level if applicable
    fc.townson_scale_level,
    
    -- Traceability arrays
    fc.all_concept_codes,
    fc.all_concept_displays,
    fc.all_source_cluster_ids

FROM {{ ref('fct_person_diabetes_register') }} d
LEFT JOIN {{ ref('int_foot_examination_latest') }} fc
    ON d.person_id = fc.person_id

ORDER BY d.person_id 