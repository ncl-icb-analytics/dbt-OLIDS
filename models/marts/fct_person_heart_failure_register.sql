{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- Heart Failure Register (QOF Pattern 4: Type Classification Register)
-- Business Logic: Active HF diagnosis + Subtype classification (General HF vs HF with LVSD/Reduced EF)
-- Multiple registers: Both general HF and LVSD-specific registers

WITH heart_failure_diagnoses AS (
    SELECT
        person_id,
        
        -- Person-level aggregation from observation-level data
        MIN(CASE WHEN is_heart_failure_diagnosis_code THEN clinical_effective_date END) AS earliest_hf_diagnosis_date,
        MAX(CASE WHEN is_heart_failure_diagnosis_code THEN clinical_effective_date END) AS latest_hf_diagnosis_date,
        MAX(CASE WHEN is_heart_failure_resolved_code THEN clinical_effective_date END) AS latest_hf_resolved_date,
        
        -- LVSD-specific dates
        MIN(CASE WHEN is_hf_lvsd_code THEN clinical_effective_date END) AS earliest_hf_lvsd_diagnosis_date,
        MAX(CASE WHEN is_hf_lvsd_code THEN clinical_effective_date END) AS latest_hf_lvsd_diagnosis_date,
        
        -- Reduced ejection fraction dates
        MIN(CASE WHEN is_reduced_ef_code THEN clinical_effective_date END) AS earliest_reduced_ef_diagnosis_date,
        MAX(CASE WHEN is_reduced_ef_code THEN clinical_effective_date END) AS latest_reduced_ef_diagnosis_date,
        
        -- QOF register logic: active HF diagnosis required
        CASE
            WHEN MAX(CASE WHEN is_heart_failure_diagnosis_code THEN clinical_effective_date END) IS NOT NULL 
                AND (MAX(CASE WHEN is_heart_failure_resolved_code THEN clinical_effective_date END) IS NULL 
                     OR MAX(CASE WHEN is_heart_failure_diagnosis_code THEN clinical_effective_date END) > 
                        MAX(CASE WHEN is_heart_failure_resolved_code THEN clinical_effective_date END))
            THEN TRUE
            ELSE FALSE
        END AS has_active_hf_diagnosis,
        
        -- Subtype flags
        CASE 
            WHEN MAX(CASE WHEN is_hf_lvsd_code THEN clinical_effective_date END) IS NOT NULL 
              OR MAX(CASE WHEN is_reduced_ef_code THEN clinical_effective_date END) IS NOT NULL
            THEN TRUE 
            ELSE FALSE 
        END AS has_lvsd_diagnosis,
        
        CASE 
            WHEN MAX(CASE WHEN is_reduced_ef_code THEN clinical_effective_date END) IS NOT NULL
            THEN TRUE 
            ELSE FALSE 
        END AS has_reduced_ef_diagnosis,
        
        -- Traceability arrays
        ARRAY_AGG(DISTINCT CASE WHEN is_heart_failure_diagnosis_code THEN concept_code ELSE NULL END) AS all_hf_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_heart_failure_diagnosis_code THEN concept_display ELSE NULL END) AS all_hf_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_heart_failure_resolved_code THEN concept_code ELSE NULL END) AS all_resolved_concept_codes
        
    FROM {{ ref('int_heart_failure_diagnoses_all') }}
    GROUP BY person_id
),

register_logic AS (
    SELECT
        p.person_id,
        age.age,
        
        -- No age restrictions for HF register
        TRUE AS meets_age_criteria,
        
        -- Diagnosis component
        COALESCE(diag.has_active_hf_diagnosis, FALSE) AS has_active_diagnosis,
        
        -- General HF register inclusion
        CASE
            WHEN diag.has_active_hf_diagnosis = TRUE
            THEN TRUE
            ELSE FALSE
        END AS is_on_hf_register,
        
        -- LVSD/Reduced EF register inclusion (subset of general HF register)
        CASE
            WHEN diag.has_active_hf_diagnosis = TRUE
                AND (diag.has_lvsd_diagnosis = TRUE OR diag.has_reduced_ef_diagnosis = TRUE)
            THEN TRUE
            ELSE FALSE
        END AS is_on_hf_lvsd_reduced_ef_register,
        
        -- Clinical dates
        diag.earliest_hf_diagnosis_date,
        diag.latest_hf_diagnosis_date,
        diag.latest_hf_resolved_date,
        diag.earliest_hf_lvsd_diagnosis_date,
        diag.latest_hf_lvsd_diagnosis_date,
        diag.earliest_reduced_ef_diagnosis_date,
        diag.latest_reduced_ef_diagnosis_date,
        
        -- Subtype flags
        diag.has_lvsd_diagnosis,
        diag.has_reduced_ef_diagnosis,
        
        -- Traceability
        diag.all_hf_concept_codes,
        diag.all_hf_concept_displays,
        diag.all_resolved_concept_codes
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN heart_failure_diagnoses diag ON p.person_id = diag.person_id
)

-- Final selection: Only individuals on heart failure register
SELECT
    person_id,
    age,
    is_on_hf_register,
    is_on_hf_lvsd_reduced_ef_register,
    
    -- Clinical diagnosis dates
    earliest_hf_diagnosis_date,
    latest_hf_diagnosis_date,
    latest_hf_resolved_date,
    
    -- LVSD/Reduced EF specific dates
    earliest_hf_lvsd_diagnosis_date,
    latest_hf_lvsd_diagnosis_date,
    earliest_reduced_ef_diagnosis_date,
    latest_reduced_ef_diagnosis_date,
    
    -- Subtype classification flags
    has_lvsd_diagnosis,
    has_reduced_ef_diagnosis,
    
    -- Traceability for audit
    all_hf_concept_codes,
    all_hf_concept_displays,
    all_resolved_concept_codes,
    
    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis
FROM register_logic
WHERE is_on_hf_register = TRUE 