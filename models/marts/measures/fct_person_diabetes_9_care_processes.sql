{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['person_id'], 'unique': false},
            {'columns': ['person_id', 'care_processes_9_completed'], 'unique': false}
        ]
    )
}}

WITH twelve_months_ago AS (
    SELECT DATEADD(month, -12, CURRENT_DATE()) AS twelve_months_ago
)

SELECT
    eight.person_id,
    
    -- Copy all fields from 8 processes
    eight.latest_hba1c_date,
    eight.hba1c_completed_in_last_12m,
    eight.latest_hba1c_value,
    
    eight.latest_bp_date,
    eight.bp_completed_in_last_12m,
    
    eight.latest_cholesterol_date,
    eight.cholesterol_completed_in_last_12m,
    
    eight.latest_creatinine_date,
    eight.creatinine_completed_in_last_12m,
    
    eight.latest_acr_date,
    eight.acr_completed_in_last_12m,
    
    eight.latest_foot_check_date,
    eight.foot_check_completed_in_last_12m,
    
    eight.latest_bmi_date,
    eight.bmi_completed_in_last_12m,
    
    eight.latest_smoking_date,
    eight.smoking_completed_in_last_12m,
    
    -- Add retinal screening (9th process)
    ret.clinical_effective_date AS latest_retinal_screening_date,
    CASE WHEN ret.clinical_effective_date >= t.twelve_months_ago THEN TRUE ELSE FALSE END AS retinal_screening_completed_in_last_12m,
    
    -- Overall completion metrics
    eight.care_processes_completed AS care_processes_8_completed,
    eight.care_processes_completed + 
        CASE WHEN ret.clinical_effective_date >= t.twelve_months_ago THEN 1 ELSE 0 END AS care_processes_9_completed,
    eight.all_processes_completed AS all_8_processes_completed,
    CASE WHEN eight.all_processes_completed AND 
              ret.clinical_effective_date >= t.twelve_months_ago 
         THEN TRUE ELSE FALSE 
    END AS all_9_processes_completed

FROM {{ ref('fct_person_diabetes_8_care_processes') }} eight
CROSS JOIN twelve_months_ago t
LEFT JOIN {{ ref('int_retinal_screening_latest') }} ret
    ON eight.person_id = ret.person_id

ORDER BY eight.person_id 