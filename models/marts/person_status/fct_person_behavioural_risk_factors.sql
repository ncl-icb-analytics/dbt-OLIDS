{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Person-level behavioural risk factors for complete population.
Provides comprehensive view of modifiable lifestyle factors for population health management.

Business Logic:
- Includes ALL persons from dim_person_demographics (persons with recorded birth dates)
- Combines latest BMI category, smoking status, and alcohol status where available
- NULL values for persons without assessments, with explicit data availability flags
- Includes risk sort keys for each factor to enable risk stratification
- One row per person in the population (matching dim_person_demographics grain)
*/

WITH all_persons AS (
    SELECT person_id 
    FROM {{ ref('dim_person_demographics') }}
),

bmi_status AS (
    SELECT
        person_id,
        bmi_category,
        bmi_value,
        bmi_risk_sort_key
    FROM {{ ref('int_bmi_latest') }}
),

smoking_status AS (
    SELECT
        person_id,
        smoking_status,
        smoking_risk_sort_key
    FROM {{ ref('fct_person_smoking_status') }}
),

alcohol_status AS (
    SELECT
        person_id,
        alcohol_status,
        alcohol_risk_sort_key
    FROM {{ ref('fct_person_alcohol_status') }}
)

SELECT
    p.person_id,
    
    -- BMI status (NULL if no assessment)
    b.bmi_category,
    b.bmi_value,
    b.bmi_risk_sort_key,
    
    -- Smoking status (NULL if no assessment)
    s.smoking_status,
    s.smoking_risk_sort_key,
    
    -- Alcohol status (NULL if no assessment)
    a.alcohol_status,
    a.alcohol_risk_sort_key,
    
    -- Combined risk indicators (only count where data exists)
    (CASE WHEN b.bmi_risk_sort_key > 2 THEN 1 ELSE 0 END +  -- Overweight or obese
     CASE WHEN s.smoking_risk_sort_key = 3 THEN 1 ELSE 0 END +  -- Current smoker
     CASE WHEN a.alcohol_risk_sort_key >= 3 THEN 1 ELSE 0 END) AS risk_factor_count,  -- Increasing risk or higher
    
    -- Data completeness flags (explicit TRUE/FALSE for all persons)
    CASE WHEN b.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_bmi_data,
    CASE WHEN s.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_smoking_data,
    CASE WHEN a.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_alcohol_data

FROM all_persons p
LEFT JOIN bmi_status b ON p.person_id = b.person_id
LEFT JOIN smoking_status s ON p.person_id = s.person_id
LEFT JOIN alcohol_status a ON p.person_id = a.person_id

ORDER BY person_id