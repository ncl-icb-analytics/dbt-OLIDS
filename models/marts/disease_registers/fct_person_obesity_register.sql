{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- Obesity Register (QOF Pattern 6: Complex Clinical Logic)
-- Business Logic: Age ≥18 + BMI ≥30 OR (BAME + BMI ≥27.5)
-- Complex Logic: Ethnicity-specific BMI thresholds for register inclusion

WITH bmi_data AS (
    SELECT
        person_id,
        is_bmi_30_plus,
        is_bmi_27_5_plus,
        latest_bmi_date,
        latest_valid_bmi_date,
        latest_valid_bmi_value,
        all_bmi_concept_codes,
        all_bmi_concept_displays
    FROM {{ ref('int_bmi_qof') }}
),

ethnicity_data AS (
    SELECT
        person_id,
        is_bame,
        latest_ethnicity_date,
        latest_bame_date,
        all_ethnicity_concept_codes,
        all_ethnicity_concept_displays
    FROM {{ ref('int_ethnicity_qof') }}
),

register_logic AS (
    SELECT
        p.person_id,
        
        -- Age restriction: ≥18 years for obesity register
        CASE WHEN age.age >= 18 THEN TRUE ELSE FALSE END AS meets_age_criteria,
        
        -- BMI and ethnicity components
        COALESCE(bmi.is_bmi_30_plus, FALSE) AS has_bmi_30_plus,
        COALESCE(bmi.is_bmi_27_5_plus, FALSE) AS has_bmi_27_5_plus,
        COALESCE(eth.is_bame, FALSE) AS is_bame,
        
        -- Complex inclusion logic: BMI ≥30 OR (BAME + BMI ≥27.5)
        CASE
            WHEN age.age >= 18 AND (
                bmi.is_bmi_30_plus = TRUE OR 
                (eth.is_bame = TRUE AND bmi.is_bmi_27_5_plus = TRUE)
            )
            THEN TRUE
            ELSE FALSE
        END AS is_on_register,
        
        -- BMI data
        bmi.latest_bmi_date,
        bmi.latest_valid_bmi_date,
        bmi.latest_valid_bmi_value,
        bmi.all_bmi_concept_codes,
        bmi.all_bmi_concept_displays,
        
        -- Ethnicity data
        eth.latest_ethnicity_date,
        eth.latest_bame_date,
        eth.all_ethnicity_concept_codes,
        eth.all_ethnicity_concept_displays,
        
        -- Person demographics
        age.age
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN bmi_data bmi ON p.person_id = bmi.person_id
    LEFT JOIN ethnicity_data eth ON p.person_id = eth.person_id
)

-- Final selection: Only individuals meeting obesity register criteria
SELECT
    person_id,
    age,
    is_on_register,
    
    -- Clinical criteria flags
    meets_age_criteria,
    has_bmi_30_plus,
    has_bmi_27_5_plus,
    is_bame,
    
    -- BMI measurements
    latest_bmi_date,
    latest_valid_bmi_date,
    latest_valid_bmi_value,
    
    -- Ethnicity information
    latest_ethnicity_date,
    latest_bame_date,
    
    -- Traceability for audit
    all_bmi_concept_codes,
    all_bmi_concept_displays,
    all_ethnicity_concept_codes,
    all_ethnicity_concept_displays
FROM register_logic
WHERE is_on_register = TRUE 