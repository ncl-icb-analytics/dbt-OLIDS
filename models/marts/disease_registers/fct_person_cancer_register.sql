{{
    config(
        materialized='table'
    )
}}

/*
Cancer Register - QOF Quality Measures
Tracks all patients with cancer diagnoses on/after April 1, 2003.

Simple Register Pattern with Date Filter:
- Presence of cancer diagnosis on/after 1 April 2003 = on register
- No resolution codes (cancer is permanent)
- No age restrictions
- Excludes non-melanotic skin cancers (handled in cluster definition)

QOF Business Rules:
1. Cancer diagnosis (CAN_COD) on/after 1 April 2003 qualifies for register
2. Cancer is considered a permanent condition - no resolution
3. Used for survivorship care and follow-up monitoring
4. Supports cancer care quality measures

Note: Legacy includes episode timing flags, but keeping this model simple 
per architectural guidance. Episode analysis can be done separately if needed.

Matches legacy fct_person_dx_cancer business logic and field structure.
*/

WITH cancer_diagnoses AS (
    SELECT
        person_id,
        
        -- Person-level aggregation from observation-level data
        MIN(CASE WHEN is_cancer_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
        MAX(CASE WHEN is_cancer_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
        MAX(CASE WHEN is_cancer_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- QOF register logic: active diagnosis required since April 2003
        CASE
            WHEN MAX(CASE WHEN is_cancer_diagnosis_code THEN clinical_effective_date END) IS NOT NULL 
                AND MAX(CASE WHEN is_cancer_diagnosis_code THEN clinical_effective_date END) >= '2003-04-01'
                AND (MAX(CASE WHEN is_cancer_resolved_code THEN clinical_effective_date END) IS NULL 
                     OR MAX(CASE WHEN is_cancer_diagnosis_code THEN clinical_effective_date END) > 
                        MAX(CASE WHEN is_cancer_resolved_code THEN clinical_effective_date END))
            THEN TRUE
            ELSE FALSE
        END AS has_active_cancer_diagnosis,
        
        -- Count of cancer episodes
        COUNT(DISTINCT CASE WHEN is_cancer_diagnosis_code THEN clinical_effective_date END) AS total_cancer_episodes,
        
        -- Traceability arrays
        ARRAY_AGG(DISTINCT CASE WHEN is_cancer_diagnosis_code THEN concept_code ELSE NULL END) AS all_cancer_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_cancer_diagnosis_code THEN concept_display ELSE NULL END) AS all_cancer_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_cancer_resolved_code THEN concept_code ELSE NULL END) AS all_resolved_concept_codes
        
    FROM {{ ref('int_cancer_diagnoses_all') }}
    GROUP BY person_id
),

-- Add person demographics matching legacy structure
final AS (
    SELECT
        cd.person_id,
        age.age,
        
        -- Register flag (always true after date filtering)
        cd.has_active_cancer_diagnosis AS is_on_cancer_register,
        
        -- Diagnosis dates
        cd.earliest_diagnosis_date,
        cd.latest_diagnosis_date,
        cd.latest_resolved_date,
        
        -- Code arrays for traceability  
        cd.all_cancer_concept_codes,
        cd.all_cancer_concept_displays,
        cd.all_resolved_concept_codes
        
    FROM cancer_diagnoses cd
    LEFT JOIN {{ ref('dim_person') }} p ON cd.person_id = p.person_id
    LEFT JOIN {{ ref('dim_person_age') }} age ON cd.person_id = age.person_id
    WHERE cd.has_active_cancer_diagnosis = TRUE  -- Only include persons with active cancer diagnosis
)

SELECT 
    person_id,
    age,
    is_on_cancer_register,
    earliest_diagnosis_date,
    latest_diagnosis_date,
    latest_resolved_date,
    all_cancer_concept_codes,
    all_cancer_concept_displays,
    all_resolved_concept_codes
FROM final 