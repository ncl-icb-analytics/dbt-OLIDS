{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
**Learning Disability Register - QOF Quality Measures**

Pattern 1: Simple Register with Age Filter

Business Logic:
- Learning disability diagnosis (LD_DIAGNOSIS_COD) for age ≥14 years
- No resolution codes (LD is permanent condition)
- Age restriction: patients must be 14+ years
- Based on legacy fct_person_dx_ld.sql

QOF Context:
Used for learning disability quality measures including:
- Learning disability care pathway monitoring
- Health equity assessment and improvement
- Special needs service coordination
- Annual health checks

Matches legacy business logic and field structure with simplification.
*/

WITH learning_disability_diagnoses AS (
    
    SELECT
        ld.person_id,
        ld.earliest_ld_diagnosis_date,
        ld.latest_ld_diagnosis_date,
        ld.all_ld_concept_codes,
        ld.all_ld_concept_displays
        
    FROM {{ ref('int_learning_disability_diagnoses_all') }} ld
    WHERE ld.has_learning_disability_diagnosis = TRUE
),

register_logic AS (
    
    SELECT
        ld.*,
        p.sk_patient_id,
        age.age,
        
        -- QOF Register Logic: LD diagnosis + age ≥14
        (age.age >= 14) AS is_on_ld_register
        
    FROM learning_disability_diagnoses ld
    INNER JOIN {{ ref('dim_person') }} p
        ON ld.person_id = p.person_id
    INNER JOIN {{ ref('dim_person_age') }} age
        ON ld.person_id = age.person_id
)

-- Final selection: Only include patients on the learning disability register
SELECT
    rl.person_id,
    rl.sk_patient_id,
    rl.age,
    rl.is_on_ld_register,
    rl.earliest_ld_diagnosis_date,
    rl.latest_ld_diagnosis_date,
    rl.all_ld_concept_codes,
    rl.all_ld_concept_displays

FROM register_logic rl
WHERE rl.is_on_ld_register = TRUE 