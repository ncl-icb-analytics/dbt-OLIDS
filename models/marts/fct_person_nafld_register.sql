{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- Non-Alcoholic Fatty Liver Disease (NAFLD) Register (QOF Pattern 1: Simple Register)
-- Business Logic: Simple presence of NAFLD diagnosis code = on register
-- Uses hardcoded concept codes (no cluster ID available in terminology mapping)

WITH nafld_diagnoses AS (
    SELECT
        person_id,
        earliest_diagnosis_date AS earliest_nafld_diagnosis_date,
        latest_diagnosis_date AS latest_nafld_diagnosis_date,
        
        -- Simple register logic: presence of any diagnosis = on register
        TRUE AS is_on_nafld_register,
        
        -- Traceability arrays
        all_diagnosis_concept_codes,
        all_diagnosis_concept_displays
    FROM {{ ref('int_nafld_diagnoses_all') }}
),

register_logic AS (
    SELECT
        p.person_id,
        
        -- No age restriction for NAFLD register
        TRUE AS meets_criteria,
        
        -- Simple inclusion logic: presence of diagnosis
        CASE
            WHEN diag.is_on_nafld_register = TRUE
            THEN TRUE
            ELSE FALSE
        END AS is_on_nafld_register,
        
        -- Clinical dates
        diag.earliest_nafld_diagnosis_date,
        diag.latest_nafld_diagnosis_date,
        
        -- Traceability
        diag.all_diagnosis_concept_codes,
        diag.all_diagnosis_concept_displays,
        
        -- Person demographics
        age.age
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN nafld_diagnoses diag ON p.person_id = diag.person_id
)

-- Final selection: Only individuals with NAFLD diagnosis
SELECT
    person_id,
    age,
    is_on_nafld_register,
    
    -- Clinical diagnosis dates
    earliest_nafld_diagnosis_date,
    latest_nafld_diagnosis_date,
    
    -- Traceability for audit
    all_diagnosis_concept_codes,
    all_diagnosis_concept_displays,
    
    -- Criteria flags for transparency
    meets_criteria
FROM register_logic
WHERE is_on_nafld_register = TRUE 