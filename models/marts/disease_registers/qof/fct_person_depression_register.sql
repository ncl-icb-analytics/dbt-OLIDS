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
        person_id,
        
        -- Person-level aggregation from observation-level data
        MIN(CASE WHEN is_depression_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
        MAX(CASE WHEN is_depression_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
        MAX(CASE WHEN is_depression_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- QOF register logic: active diagnosis required since April 2006
        CASE
            WHEN MAX(CASE WHEN is_depression_diagnosis_code THEN clinical_effective_date END) IS NOT NULL 
                AND MAX(CASE WHEN is_depression_diagnosis_code THEN clinical_effective_date END) >= '2006-04-01'
                AND (MAX(CASE WHEN is_depression_resolved_code THEN clinical_effective_date END) IS NULL 
                     OR MAX(CASE WHEN is_depression_diagnosis_code THEN clinical_effective_date END) > 
                        MAX(CASE WHEN is_depression_resolved_code THEN clinical_effective_date END))
            THEN TRUE
            ELSE FALSE
        END AS has_active_depression_diagnosis,
        
        -- QOF temporal flags for recent episodes
        CASE 
            WHEN MAX(CASE WHEN is_depression_diagnosis_code THEN clinical_effective_date END) >= CURRENT_DATE - INTERVAL '12 months' 
            THEN TRUE 
            ELSE FALSE 
        END AS has_episode_last_12m,
        
        CASE 
            WHEN MAX(CASE WHEN is_depression_diagnosis_code THEN clinical_effective_date END) >= CURRENT_DATE - INTERVAL '15 months' 
            THEN TRUE 
            ELSE FALSE 
        END AS has_episode_last_15m,
        
        CASE 
            WHEN MAX(CASE WHEN is_depression_diagnosis_code THEN clinical_effective_date END) >= CURRENT_DATE - INTERVAL '24 months' 
            THEN TRUE 
            ELSE FALSE 
        END AS has_episode_last_24m,
        
        -- Traceability arrays
        ARRAY_AGG(DISTINCT CASE WHEN is_depression_diagnosis_code THEN concept_code ELSE NULL END) AS all_depression_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_depression_diagnosis_code THEN concept_display ELSE NULL END) AS all_depression_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_depression_resolved_code THEN concept_code ELSE NULL END) AS all_resolved_concept_codes
        
    FROM {{ ref('int_depression_diagnoses_all') }}
    GROUP BY person_id
),

register_logic AS (
    
    SELECT
        dd.*,
        age.age,
        
        -- QOF Register Logic: Age ≥18 + Active depression diagnosis
        (
            age.age >= 18
            AND dd.has_active_depression_diagnosis = TRUE
        ) AS is_on_register
        
    FROM depression_diagnoses dd
    INNER JOIN {{ ref('dim_person') }} p
        ON dd.person_id = p.person_id
    INNER JOIN {{ ref('dim_person_age') }} age
        ON dd.person_id = age.person_id
    WHERE dd.has_active_depression_diagnosis = TRUE  -- Only include persons with active depression diagnosis
)

-- Final selection: Only include patients on the depression register
SELECT
    rl.person_id,
    rl.age,
    rl.is_on_register,
    rl.earliest_diagnosis_date,
    rl.latest_diagnosis_date,
    rl.latest_resolved_date,
    rl.all_depression_concept_codes,
    rl.all_depression_concept_displays,
    rl.all_resolved_concept_codes AS all_depression_resolved_concept_codes

FROM register_logic rl
WHERE rl.is_on_register = TRUE 