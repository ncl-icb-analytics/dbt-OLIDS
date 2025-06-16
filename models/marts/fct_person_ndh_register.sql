{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- Non-Diabetic Hyperglycaemia (NDH) Register (QOF Pattern 4: Type Classification Register)
-- Business Logic: Age ≥18 + NDH diagnosis + Complex diabetes exclusion logic 
-- Complex Logic: Has NDH AND (never had diabetes OR diabetes is resolved)

WITH ndh_diagnoses AS (
    SELECT
        person_id,
        
        -- NDH diagnosis dates (includes NDH, IGT, PRD)
        earliest_ndh_date,
        latest_ndh_date,
        earliest_igt_date,
        latest_igt_date,
        earliest_prd_date,
        latest_prd_date,
        earliest_multndh_date, -- Earliest of any NDH/IGT/PRD
        latest_multndh_date,   -- Latest of any NDH/IGT/PRD
        
        -- Flags for NDH subtypes
        CASE WHEN earliest_ndh_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_ndh_diagnosis,
        CASE WHEN earliest_igt_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_igt_diagnosis,
        CASE WHEN earliest_prd_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_prd_diagnosis,
        
        -- Traceability arrays
        all_ndh_concept_codes,
        all_ndh_concept_displays,
        all_igt_concept_codes,
        all_igt_concept_displays,
        all_prd_concept_codes,
        all_prd_concept_displays
    FROM {{ ref('int_ndh_diagnoses_all') }}
),

diabetes_status AS (
    SELECT
        person_id,
        
        -- Diabetes dates
        earliest_diagnosis_date AS earliest_diabetes_date,
        latest_diagnosis_date AS latest_diabetes_date,
        latest_resolved_date AS latest_diabetes_resolved_date,
        
        -- Diabetes flags
        CASE WHEN earliest_diagnosis_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_diabetes_diagnosis,
        CASE 
            WHEN latest_diagnosis_date IS NOT NULL 
                AND (latest_resolved_date IS NULL OR latest_diagnosis_date > latest_resolved_date)
            THEN FALSE -- Active diabetes
            WHEN latest_diabetes_resolved_date IS NOT NULL 
                AND latest_diabetes_resolved_date > latest_diagnosis_date
            THEN TRUE  -- Resolved diabetes
            ELSE FALSE
        END AS is_diabetes_resolved,
        
        -- Traceability
        all_diagnosis_concept_codes AS all_diabetes_concept_codes,
        all_diagnosis_concept_displays AS all_diabetes_concept_displays
    FROM {{ ref('int_diabetes_diagnoses_all') }}
),

register_logic AS (
    SELECT
        p.person_id,
        
        -- Age restriction: ≥18 years for NDH register
        CASE WHEN age.age >= 18 THEN TRUE ELSE FALSE END AS meets_age_criteria,
        
        -- NDH component
        CASE WHEN ndh.earliest_multndh_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_ndh_diagnosis,
        
        -- Diabetes exclusion logic
        COALESCE(dm.has_diabetes_diagnosis, FALSE) AS has_diabetes_diagnosis,
        COALESCE(dm.is_diabetes_resolved, FALSE) AS is_diabetes_resolved,
        
        -- Final register inclusion: Age + NDH + (never diabetes OR diabetes resolved)
        CASE
            WHEN age.age >= 18
                AND ndh.earliest_multndh_date IS NOT NULL
                AND (dm.earliest_diabetes_date IS NULL OR dm.is_diabetes_resolved = TRUE)
            THEN TRUE
            ELSE FALSE
        END AS is_on_ndh_register,
        
        -- Clinical dates
        ndh.earliest_ndh_date,
        ndh.latest_ndh_date,
        ndh.earliest_igt_date,
        ndh.latest_igt_date,
        ndh.earliest_prd_date,
        ndh.latest_prd_date,
        ndh.earliest_multndh_date,
        ndh.latest_multndh_date,
        dm.earliest_diabetes_date,
        dm.latest_diabetes_date,
        dm.latest_diabetes_resolved_date,
        
        -- Subtype flags
        ndh.has_ndh_diagnosis AS has_specific_ndh_diagnosis,
        ndh.has_igt_diagnosis,
        ndh.has_prd_diagnosis,
        
        -- Traceability
        ndh.all_ndh_concept_codes,
        ndh.all_ndh_concept_displays,
        ndh.all_igt_concept_codes,
        ndh.all_igt_concept_displays,
        ndh.all_prd_concept_codes,
        ndh.all_prd_concept_displays,
        dm.all_diabetes_concept_codes,
        dm.all_diabetes_concept_displays,
        
        -- Person demographics
        age.age
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN ndh_diagnoses ndh ON p.person_id = ndh.person_id
    LEFT JOIN diabetes_status dm ON p.person_id = dm.person_id
)

-- Final selection: Only individuals on NDH register
SELECT
    person_id,
    age,
    is_on_ndh_register,
    
    -- Clinical diagnosis dates
    earliest_ndh_date,
    latest_ndh_date,
    earliest_igt_date,
    latest_igt_date,
    earliest_prd_date,
    latest_prd_date,
    earliest_multndh_date,
    latest_multndh_date,
    
    -- Diabetes context dates
    earliest_diabetes_date,
    latest_diabetes_date,
    latest_diabetes_resolved_date,
    
    -- Subtype classification flags
    has_specific_ndh_diagnosis,
    has_igt_diagnosis,
    has_prd_diagnosis,
    has_diabetes_diagnosis,
    is_diabetes_resolved,
    
    -- Traceability for audit
    all_ndh_concept_codes,
    all_ndh_concept_displays,
    all_igt_concept_codes,
    all_igt_concept_displays,
    all_prd_concept_codes,
    all_prd_concept_displays,
    all_diabetes_concept_codes,
    all_diabetes_concept_displays,
    
    -- Criteria flags for transparency
    meets_age_criteria,
    has_ndh_diagnosis
FROM register_logic
WHERE is_on_ndh_register = TRUE 