{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['person_id'], 'unique': false},
            {'columns': ['person_id', 'is_on_obesity_register'], 'unique': false}
        ]
    )
}}

WITH base_population AS (
    -- Get all people aged 18+ with BMI or ethnicity data
    SELECT
        COALESCE(b.person_id, e.person_id) AS person_id,
        COALESCE(b.sk_patient_id, e.sk_patient_id) AS sk_patient_id,
        age.age,
        
        -- BMI data
        b.is_bmi_30_plus,
        b.is_bmi_27_5_plus,
        b.earliest_bmi_date,
        b.latest_bmi_date,
        b.latest_valid_bmi_date,
        b.latest_valid_bmi_value,
        b.all_bmi_concept_codes,
        b.all_bmi_concept_displays,
        
        -- Ethnicity data
        e.is_bame,
        e.latest_ethnicity_date,
        e.latest_bame_date,
        e.all_ethnicity_concept_codes,
        e.all_ethnicity_concept_displays
        
    FROM {{ ref('int_bmi_qof') }} b
    FULL OUTER JOIN {{ ref('int_ethnicity_qof') }} e
        ON b.person_id = e.person_id
    JOIN {{ ref('dim_person_age') }} age
        ON COALESCE(b.person_id, e.person_id) = age.person_id
    WHERE age.age >= 18 -- Rule 1: Age filter
)

SELECT
    person_id,
    sk_patient_id,
    age,
    
    -- Obesity register inclusion rules
    CASE
        WHEN is_bmi_30_plus THEN TRUE -- Rule 2: BMI >= 30 (any ethnicity)
        WHEN is_bame AND is_bmi_27_5_plus THEN TRUE -- Rule 3: BAME with BMI >= 27.5
        ELSE FALSE
    END AS is_on_obesity_register,
    
    -- Individual flags
    COALESCE(is_bame, FALSE) AS is_bame,
    COALESCE(is_bmi_30_plus, FALSE) AS has_bmi_30_plus,
    COALESCE(is_bmi_27_5_plus, FALSE) AS has_bmi_27_5_plus,
    
    -- BMI dates and values
    earliest_bmi_date,
    latest_bmi_date,
    latest_valid_bmi_date,
    latest_valid_bmi_value,
    
    -- Ethnicity dates
    latest_ethnicity_date,
    latest_bame_date,
    
    -- Concept arrays
    all_bmi_concept_codes,
    all_bmi_concept_displays,
    all_ethnicity_concept_codes,
    all_ethnicity_concept_displays

FROM base_population
WHERE (is_bmi_30_plus OR (is_bame AND is_bmi_27_5_plus)) -- Only include patients on the obesity register

ORDER BY person_id 