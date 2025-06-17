{{
    config(
        materialized='table'
    )
}}

/*
Gestational Diabetes Register - QOF Quality Measures
Tracks all patients with gestational diabetes diagnoses.

Simple Register Pattern:
- Presence of gestational diabetes diagnosis = on register
- No resolution codes (important for future diabetes risk)
- No age restrictions
- Important for pregnancy care and future diabetes monitoring

QOF Business Rules:
1. Any gestational diabetes diagnosis code qualifies for register inclusion
2. Gestational diabetes records are permanent for future risk assessment
3. Used for pregnancy care quality measures
4. Future diabetes risk monitoring and prevention

Matches legacy fct_person_dx_gestational_diabetes business logic and field structure.
*/

WITH base_diagnoses AS (
    SELECT 
        person_id,
        earliest_gestational_diabetes_date,
        latest_gestational_diabetes_date,
        total_gestational_diabetes_episodes,
        all_gestational_diabetes_concept_codes,
        all_gestational_diabetes_concept_displays
    FROM {{ ref('int_gestational_diabetes_diagnoses_all') }}
),

-- Add person demographics matching legacy structure
final AS (
    SELECT
        bd.person_id,
        age.age,
        
        -- Register flag (always true for simple register pattern)
        TRUE AS is_on_gestational_diabetes_register,
        
        -- Diagnosis dates
        bd.earliest_gestational_diabetes_date,
        bd.latest_gestational_diabetes_date,
        
        -- Code arrays for traceability  
        bd.all_gestational_diabetes_concept_codes,
        bd.all_gestational_diabetes_concept_displays
        
    FROM base_diagnoses bd
    LEFT JOIN {{ ref('dim_person') }} p ON bd.person_id = p.person_id
    LEFT JOIN {{ ref('dim_person_age') }} age ON bd.person_id = age.person_id
)

SELECT * FROM final 