{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['person_id'], 'unique': false},
            {'columns': ['person_id', 'care_processes_completed'], 'unique': false}
        ]
    )
}}

WITH twelve_months_ago AS (
    SELECT DATEADD(month, -12, CURRENT_DATE()) AS twelve_months_ago
),

diabetes_register AS (
    -- Base population: people on the diabetes register (one row per person)
    SELECT 
        person_id
    FROM {{ ref('fct_person_diabetes_register') }}
    -- No WHERE clause needed - this table already filters to people on the register
),

care_process_data AS (
    SELECT
        dr.person_id,
        
        -- HbA1c
        hba.clinical_effective_date AS latest_hba1c_date,
        CASE WHEN hba.clinical_effective_date >= t.twelve_months_ago THEN TRUE ELSE FALSE END AS hba1c_completed_in_last_12m,
        hba.hba1c_value AS latest_hba1c_value,
        
        -- Blood Pressure
        bp.clinical_effective_date AS latest_bp_date,
        CASE WHEN bp.clinical_effective_date >= t.twelve_months_ago THEN TRUE ELSE FALSE END AS bp_completed_in_last_12m,
        
        -- Cholesterol
        chol.clinical_effective_date AS latest_cholesterol_date,
        CASE WHEN chol.clinical_effective_date >= t.twelve_months_ago THEN TRUE ELSE FALSE END AS cholesterol_completed_in_last_12m,
        
        -- Serum Creatinine
        cre.clinical_effective_date AS latest_creatinine_date,
        CASE WHEN cre.clinical_effective_date >= t.twelve_months_ago THEN TRUE ELSE FALSE END AS creatinine_completed_in_last_12m,
        
        -- Urine ACR
        acr.clinical_effective_date AS latest_acr_date,
        CASE WHEN acr.clinical_effective_date >= t.twelve_months_ago THEN TRUE ELSE FALSE END AS acr_completed_in_last_12m,
        
        -- Foot Check
        fc.clinical_effective_date AS latest_foot_check_date,
        -- Foot check completed if within 12 months AND both feet checked OR one foot checked and other absent/amputated AND not declined/unsuitable
        CASE
            WHEN fc.clinical_effective_date IS NOT NULL 
                AND fc.clinical_effective_date >= t.twelve_months_ago
                AND (
                    fc.both_feet_checked 
                    OR (fc.left_foot_checked AND (fc.right_foot_absent OR fc.right_foot_amputated))
                    OR (fc.right_foot_checked AND (fc.left_foot_absent OR fc.left_foot_amputated))
                )
                AND NOT (fc.is_unsuitable OR fc.is_declined)
            THEN TRUE
            ELSE FALSE
        END AS foot_check_completed_in_last_12m,
        
        -- BMI
        bmi.clinical_effective_date AS latest_bmi_date,
        CASE WHEN bmi.clinical_effective_date >= t.twelve_months_ago THEN TRUE ELSE FALSE END AS bmi_completed_in_last_12m,
        
        -- Smoking
        smok.clinical_effective_date AS latest_smoking_date,
        CASE WHEN smok.clinical_effective_date >= t.twelve_months_ago THEN TRUE ELSE FALSE END AS smoking_completed_in_last_12m
        
    FROM diabetes_register dr
    CROSS JOIN twelve_months_ago t
    LEFT JOIN {{ ref('int_hba1c_latest') }} hba
        ON dr.person_id = hba.person_id
    LEFT JOIN {{ ref('int_blood_pressure_latest') }} bp
        ON dr.person_id = bp.person_id
    LEFT JOIN {{ ref('int_cholesterol_latest') }} chol
        ON dr.person_id = chol.person_id
    LEFT JOIN {{ ref('int_creatinine_latest') }} cre
        ON dr.person_id = cre.person_id
    LEFT JOIN {{ ref('int_urine_acr_latest') }} acr
        ON dr.person_id = acr.person_id
    LEFT JOIN {{ ref('int_foot_examination_latest') }} fc
        ON dr.person_id = fc.person_id
    LEFT JOIN {{ ref('int_bmi_latest') }} bmi
        ON dr.person_id = bmi.person_id
    LEFT JOIN {{ ref('int_smoking_status_latest') }} smok
        ON dr.person_id = smok.person_id
)

SELECT
    person_id,
    
    -- Individual care process dates and completion flags
    latest_hba1c_date,
    hba1c_completed_in_last_12m,
    latest_hba1c_value,
    
    latest_bp_date,
    bp_completed_in_last_12m,
    
    latest_cholesterol_date,
    cholesterol_completed_in_last_12m,
    
    latest_creatinine_date,
    creatinine_completed_in_last_12m,
    
    latest_acr_date,
    acr_completed_in_last_12m,
    
    latest_foot_check_date,
    foot_check_completed_in_last_12m,
    
    latest_bmi_date,
    bmi_completed_in_last_12m,
    
    latest_smoking_date,
    smoking_completed_in_last_12m,
    
    -- Overall completion metrics
    (CASE WHEN hba1c_completed_in_last_12m THEN 1 ELSE 0 END +
     CASE WHEN bp_completed_in_last_12m THEN 1 ELSE 0 END +
     CASE WHEN cholesterol_completed_in_last_12m THEN 1 ELSE 0 END +
     CASE WHEN creatinine_completed_in_last_12m THEN 1 ELSE 0 END +
     CASE WHEN acr_completed_in_last_12m THEN 1 ELSE 0 END +
     CASE WHEN foot_check_completed_in_last_12m THEN 1 ELSE 0 END +
     CASE WHEN bmi_completed_in_last_12m THEN 1 ELSE 0 END +
     CASE WHEN smoking_completed_in_last_12m THEN 1 ELSE 0 END
    ) AS care_processes_completed,
    
    CASE 
        WHEN (hba1c_completed_in_last_12m AND
              bp_completed_in_last_12m AND
              cholesterol_completed_in_last_12m AND
              creatinine_completed_in_last_12m AND
              acr_completed_in_last_12m AND
              foot_check_completed_in_last_12m AND
              bmi_completed_in_last_12m AND
              smoking_completed_in_last_12m) THEN TRUE
        ELSE FALSE
    END AS all_processes_completed

FROM care_process_data
ORDER BY person_id 