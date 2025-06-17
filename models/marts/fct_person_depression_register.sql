{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
**Depression Register - QOF Mental Health Quality Measures**

Pattern 2: Standard QOF Register (Diagnosis + Resolution + Age + Date Filter)

Business Logic:
- Age ≥18 years (QOF requirement)
- Latest depression episode on/after 1 April 2006 (QOF date threshold)
- Unresolved: latest_depression_date > latest_resolved_date OR no resolved code
- Based on legacy fct_person_dx_depression.sql

QOF Context:
Used for depression quality measures including:
- Depression care pathways and treatment monitoring
- Recovery planning and review scheduling
- Psychological therapy access
- Medication management and monitoring

Matches legacy business logic and field structure with simplification (no episode timing flags).
*/

WITH depression_diagnoses AS (
    
    SELECT
        diag.person_id,
        diag.earliest_depression_date,
        diag.latest_depression_date,
        diag.latest_resolved_date,
        diag.all_depression_concept_codes,
        diag.all_depression_concept_displays,
        diag.all_resolved_concept_codes
        
    FROM {{ ref('int_depression_diagnoses_all') }} diag
    WHERE diag.has_potential_qof_depression = TRUE
),

register_logic AS (
    
    SELECT
        dd.*,
        age.age,
        
        -- QOF Register Logic: Age ≥18 + episode ≥1 April 2006 + unresolved
        (
            age.age >= 18
            AND dd.latest_depression_date >= DATE '2006-04-01'
            AND (dd.latest_resolved_date IS NULL OR dd.latest_resolved_date < dd.latest_depression_date)
        ) AS is_on_depression_register
        
    FROM depression_diagnoses dd
    INNER JOIN {{ ref('dim_person') }} p
        ON dd.person_id = p.person_id
    INNER JOIN {{ ref('dim_person_age') }} age
        ON dd.person_id = age.person_id
)

-- Final selection: Only include patients on the depression register
SELECT
    rl.person_id,
    rl.age,
    rl.is_on_depression_register,
    rl.earliest_depression_date AS earliest_depression_diagnosis_date,
    rl.latest_depression_date AS latest_depression_diagnosis_date,
    rl.latest_resolved_date AS latest_depression_resolved_date,
    rl.all_depression_concept_codes,
    rl.all_depression_concept_displays,
    rl.all_resolved_concept_codes AS all_depression_resolved_concept_codes

FROM register_logic rl
WHERE rl.is_on_depression_register = TRUE 