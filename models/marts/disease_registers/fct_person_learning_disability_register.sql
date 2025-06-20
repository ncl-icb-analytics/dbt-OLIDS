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
        person_id,
        
        -- Person-level aggregation from observation-level data
        MIN(CASE WHEN is_learning_disability_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
        MAX(CASE WHEN is_learning_disability_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
        MAX(CASE WHEN is_learning_disability_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- QOF register logic: active diagnosis required
        CASE
            WHEN MAX(CASE WHEN is_learning_disability_diagnosis_code THEN clinical_effective_date END) IS NOT NULL 
                AND (MAX(CASE WHEN is_learning_disability_resolved_code THEN clinical_effective_date END) IS NULL 
                     OR MAX(CASE WHEN is_learning_disability_diagnosis_code THEN clinical_effective_date END) > 
                        MAX(CASE WHEN is_learning_disability_resolved_code THEN clinical_effective_date END))
            THEN TRUE
            ELSE FALSE
        END AS has_active_ld_diagnosis,
        
        -- Traceability arrays
        ARRAY_AGG(DISTINCT CASE WHEN is_learning_disability_diagnosis_code THEN concept_code ELSE NULL END) AS all_ld_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_learning_disability_diagnosis_code THEN concept_display ELSE NULL END) AS all_ld_concept_displays
        
    FROM {{ ref('int_learning_disability_diagnoses_all') }}
    GROUP BY person_id
),

register_logic AS (
    
    SELECT
        ld.*,
        age.age,
        
        -- QOF Register Logic: Active LD diagnosis + age ≥14
        (ld.has_active_ld_diagnosis = TRUE AND age.age >= 14) AS is_on_ld_register
        
    FROM learning_disability_diagnoses ld
    INNER JOIN {{ ref('dim_person') }} p
        ON ld.person_id = p.person_id
    INNER JOIN {{ ref('dim_person_age') }} age
        ON ld.person_id = age.person_id
    WHERE ld.has_active_ld_diagnosis = TRUE  -- Only include persons with active LD diagnosis
)

-- Final selection: Only include patients on the learning disability register
SELECT
    rl.person_id,
    rl.age,
    rl.is_on_ld_register,
    rl.earliest_diagnosis_date,
    rl.latest_diagnosis_date,
    rl.all_ld_concept_codes,
    rl.all_ld_concept_displays

FROM register_logic rl
WHERE rl.is_on_ld_register = TRUE 