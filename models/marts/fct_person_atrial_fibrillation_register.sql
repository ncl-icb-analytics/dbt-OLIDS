{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- Atrial Fibrillation Register (QOF Pattern 2: Standard QOF Register with Resolution Logic)  
-- Business Logic: Active AF diagnosis (latest AFIB_COD > latest AFIBRES_COD OR no resolution recorded)
-- No age restrictions or medication validation requirements

WITH af_diagnoses AS (
    SELECT
        person_id,
        earliest_diagnosis_date AS earliest_af_diagnosis_date,
        latest_diagnosis_date AS latest_af_diagnosis_date,
        latest_resolved_date AS latest_af_resolved_date,
        
        -- QOF register logic: active diagnosis required
        CASE
            WHEN latest_diagnosis_date IS NOT NULL 
                AND (latest_resolved_date IS NULL OR latest_diagnosis_date > latest_resolved_date)
            THEN TRUE
            ELSE FALSE
        END AS has_active_af_diagnosis,
        
        -- Traceability arrays
        all_diagnosis_concept_codes,
        all_diagnosis_concept_displays,
        all_source_cluster_ids
    FROM {{ ref('int_atrial_fibrillation_diagnoses_all') }}
),

register_logic AS (
    SELECT
        p.person_id,
        age.age,
        
        -- No age restrictions for AF register
        TRUE AS meets_age_criteria,
        
        -- Diagnosis component (only requirement)
        COALESCE(diag.has_active_af_diagnosis, FALSE) AS has_active_diagnosis,
        
        -- Final register inclusion: Active diagnosis required
        CASE
            WHEN diag.has_active_af_diagnosis = TRUE
            THEN TRUE
            ELSE FALSE
        END AS is_on_af_register,
        
        -- Clinical dates
        diag.earliest_af_diagnosis_date,
        diag.latest_af_diagnosis_date,
        diag.latest_af_resolved_date,
        
        -- Traceability
        diag.all_diagnosis_concept_codes,
        diag.all_diagnosis_concept_displays,
        diag.all_source_cluster_ids
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN af_diagnoses diag ON p.person_id = diag.person_id
)

-- Final selection: Only individuals with active AF diagnosis
SELECT
    person_id,
    age,
    is_on_af_register,
    
    -- Clinical diagnosis dates
    earliest_af_diagnosis_date,
    latest_af_diagnosis_date,
    latest_af_resolved_date,
    
    -- Traceability for audit
    all_diagnosis_concept_codes,
    all_diagnosis_concept_displays,
    all_source_cluster_ids,
    
    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis
FROM register_logic
WHERE is_on_af_register = TRUE 