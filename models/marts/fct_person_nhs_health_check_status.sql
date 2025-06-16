{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- NHS Health Check Status Fact Table
-- Business Logic: Eligibility and due status for NHS Health Checks

WITH nhs_health_check_eligibility AS (
    -- Basic eligibility: Age 40-74 without excluding conditions
    SELECT
        p.person_id,
        age.age,
        
        -- Check for excluding conditions (existing chronic conditions)
        CASE 
            WHEN chd.person_id IS NOT NULL 
                OR diabetes.person_id IS NOT NULL 
                OR stroke.person_id IS NOT NULL 
                OR ckd.person_id IS NOT NULL
                OR af.person_id IS NOT NULL
                OR hf.person_id IS NOT NULL
                OR fh.person_id IS NOT NULL
            THEN TRUE 
            ELSE FALSE 
        END AS has_any_excluding_condition,
        
        -- Eligibility: age 40-74 without excluding conditions
        CASE 
            WHEN age.age BETWEEN 40 AND 74 
                AND NOT (
                    chd.person_id IS NOT NULL 
                    OR diabetes.person_id IS NOT NULL 
                    OR stroke.person_id IS NOT NULL 
                    OR ckd.person_id IS NOT NULL
                    OR af.person_id IS NOT NULL
                    OR hf.person_id IS NOT NULL
                    OR fh.person_id IS NOT NULL
                )
            THEN TRUE 
            ELSE FALSE 
        END AS is_eligible_for_nhs_health_check
        
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN {{ ref('fct_person_chd_register') }} chd ON p.person_id = chd.person_id
    LEFT JOIN {{ ref('fct_person_diabetes_register') }} diabetes ON p.person_id = diabetes.person_id
    LEFT JOIN {{ ref('fct_person_stroke_tia_register') }} stroke ON p.person_id = stroke.person_id
    LEFT JOIN {{ ref('fct_person_ckd_register') }} ckd ON p.person_id = ckd.person_id
    LEFT JOIN {{ ref('fct_person_atrial_fibrillation_register') }} af ON p.person_id = af.person_id
    LEFT JOIN {{ ref('fct_person_heart_failure_register') }} hf ON p.person_id = hf.person_id
    LEFT JOIN {{ ref('fct_person_familial_hypercholesterolaemia_register') }} fh ON p.person_id = fh.person_id
)

SELECT 
    elig.person_id,
    elig.age,
    elig.has_any_excluding_condition,
    elig.is_eligible_for_nhs_health_check,
    hc.clinical_effective_date AS latest_health_check_date,
    CASE 
        WHEN hc.clinical_effective_date IS NOT NULL 
        THEN DATEDIFF(day, hc.clinical_effective_date, CURRENT_DATE()) 
        ELSE NULL 
    END AS days_since_last_health_check,
    
    -- Person is due a health check if eligible AND (never had one OR last one > 5 years ago)
    CASE
        WHEN elig.is_eligible_for_nhs_health_check = TRUE
            AND (
                hc.clinical_effective_date IS NULL 
                OR DATEDIFF(day, hc.clinical_effective_date, CURRENT_DATE()) > 1825
            )
        THEN TRUE
        ELSE FALSE
    END AS due_nhs_health_check
    
FROM nhs_health_check_eligibility elig
LEFT JOIN {{ ref('int_nhs_health_check_latest') }} hc ON elig.person_id = hc.person_id 