{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- Diabetes Register (QOF Pattern 4: Type Classification Register)
-- Business Logic: Age ≥17 + Active diabetes diagnosis + Type classification (Type 1 vs Type 2 vs Unknown)
-- Type Hierarchy: Type 1 takes precedence if both types coded on same date

WITH diabetes_diagnoses AS (
    SELECT
        person_id,
        
        -- General diabetes dates
        earliest_diabetes_date AS earliest_diabetes_diagnosis_date,
        latest_diabetes_date AS latest_diabetes_diagnosis_date,
        latest_resolved_date AS latest_diabetes_resolved_date,
        
        -- Type-specific dates (from specialized type clusters)
        earliest_type1_date AS earliest_diabetes_type1_date,
        latest_type1_date AS latest_diabetes_type1_date,
        earliest_type2_date AS earliest_diabetes_type2_date,
        latest_type2_date AS latest_diabetes_type2_date,
        
        -- QOF register logic: active diabetes diagnosis required (use existing logic)
        is_diabetes_currently_resolved = FALSE AS has_active_diabetes_diagnosis,
        
        -- Traceability arrays
        all_diabetes_concept_codes,
        all_diabetes_concept_displays,
        all_type1_concept_codes,
        all_type2_concept_codes,
        all_resolved_concept_codes
    FROM {{ ref('int_diabetes_diagnoses_all') }}
),

register_logic AS (
    SELECT
        p.person_id,
        
        -- Age restriction: ≥17 years for diabetes register
        CASE WHEN age.age >= 17 THEN TRUE ELSE FALSE END AS meets_age_criteria,
        
        -- Diagnosis component
        COALESCE(diag.has_active_diabetes_diagnosis, FALSE) AS has_active_diagnosis,
        
        -- Final register inclusion: Active diabetes required
        CASE
            WHEN age.age >= 17
                AND diag.has_active_diabetes_diagnosis = TRUE
            THEN TRUE
            ELSE FALSE
        END AS is_on_diabetes_register,
        
        -- Type classification logic (only for those on register)
        CASE
            WHEN age.age < 17 OR diag.has_active_diabetes_diagnosis != TRUE THEN NULL -- Not on register
            -- Type 1 precedence: Latest Type 1 >= Latest Type 2 (or no Type 2)
            WHEN diag.latest_diabetes_type1_date IS NOT NULL 
                AND (diag.latest_diabetes_type2_date IS NULL 
                     OR diag.latest_diabetes_type1_date >= diag.latest_diabetes_type2_date)
            THEN 'Type 1'
            -- Type 2: Latest Type 2 > Latest Type 1 (or no Type 1)
            WHEN diag.latest_diabetes_type2_date IS NOT NULL
                AND (diag.latest_diabetes_type1_date IS NULL 
                     OR diag.latest_diabetes_type2_date > diag.latest_diabetes_type1_date)
            THEN 'Type 2'
            -- Unknown: On register but no specific type codes
            ELSE 'Unknown'
        END AS diabetes_type,
        
        -- Clinical dates
        diag.earliest_diabetes_diagnosis_date,
        diag.latest_diabetes_diagnosis_date,
        diag.latest_diabetes_resolved_date,
        diag.earliest_diabetes_type1_date,
        diag.latest_diabetes_type1_date,
        diag.earliest_diabetes_type2_date,
        diag.latest_diabetes_type2_date,
        
        -- Traceability
        diag.all_diagnosis_concept_codes,
        diag.all_diagnosis_concept_displays,
        diag.all_source_cluster_ids,
        
        -- Person demographics
        age.age
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN diabetes_diagnoses diag ON p.person_id = diag.person_id
)

-- Final selection: Only individuals on diabetes register
SELECT
    person_id,
    age,
    is_on_diabetes_register,
    diabetes_type,
    
    -- Clinical diagnosis dates
    earliest_diabetes_diagnosis_date,
    latest_diabetes_diagnosis_date,
    latest_diabetes_resolved_date,
    
    -- Type-specific dates for clinical audit
    earliest_diabetes_type1_date,
    latest_diabetes_type1_date,
    earliest_diabetes_type2_date,
    latest_diabetes_type2_date,
    
    -- Traceability for audit
    all_diagnosis_concept_codes,
    all_diagnosis_concept_displays,
    all_source_cluster_ids,
    
    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis
FROM register_logic
WHERE is_on_diabetes_register = TRUE 