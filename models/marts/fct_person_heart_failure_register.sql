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
        
        -- General heart failure dates
        earliest_diagnosis_date AS earliest_hf_diagnosis_date,
        latest_diagnosis_date AS latest_hf_diagnosis_date,
        latest_resolved_date AS latest_hf_resolved_date,
        
        -- LVSD-specific dates
        earliest_lvsd_date AS earliest_hf_lvsd_diagnosis_date,
        latest_lvsd_date AS latest_hf_lvsd_diagnosis_date,
        
        -- Reduced ejection fraction dates
        earliest_reduced_ef_date AS earliest_reduced_ef_diagnosis_date,
        latest_reduced_ef_date AS latest_reduced_ef_diagnosis_date,
        
        -- QOF register logic: active HF diagnosis required
        CASE
            WHEN latest_diagnosis_date IS NOT NULL 
                AND (latest_resolved_date IS NULL OR latest_diagnosis_date > latest_resolved_date)
            THEN TRUE
            ELSE FALSE
        END AS has_active_hf_diagnosis,
        
        -- Subtype flags
        CASE WHEN earliest_lvsd_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_lvsd_diagnosis,
        CASE WHEN earliest_reduced_ef_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_reduced_ef_diagnosis,
        
        -- Traceability arrays
        all_diagnosis_concept_codes,
        all_diagnosis_concept_displays,
        all_source_cluster_ids
    FROM {{ ref('int_heart_failure_diagnoses_all') }}
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
        diag.all_diagnosis_concept_codes,
        diag.all_diagnosis_concept_displays,
        diag.all_source_cluster_ids
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
    all_diagnosis_concept_codes,
    all_diagnosis_concept_displays,
    all_source_cluster_ids,
    
    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis
FROM register_logic
WHERE is_on_hf_register = TRUE 