{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- Hypertension Register (QOF Pattern 6: Complex Clinical Logic)
-- Business Logic: Age ≥18 + Active HTN diagnosis + Clinical staging based on latest BP with context-specific NICE thresholds
-- Complex Logic: BP staging varies by measurement context (Home/ABPM vs Clinic readings)

WITH hypertension_diagnoses AS (
    SELECT
        person_id,
        earliest_diagnosis_date AS earliest_htn_diagnosis_date,
        latest_diagnosis_date AS latest_htn_diagnosis_date,
        latest_resolved_date AS latest_htn_resolved_date,
        
        -- QOF register logic: active diagnosis required
        CASE
            WHEN latest_diagnosis_date IS NOT NULL 
                AND (latest_resolved_date IS NULL OR latest_diagnosis_date > latest_resolved_date)
            THEN TRUE
            ELSE FALSE
        END AS has_active_htn_diagnosis,
        
        -- Traceability arrays
        all_diagnosis_concept_codes,
        all_diagnosis_concept_displays,
        all_source_cluster_ids
    FROM {{ ref('int_hypertension_diagnoses_all') }}
),

latest_bp_data AS (
    SELECT
        person_id,
        clinical_effective_date AS latest_bp_date,
        systolic_value AS latest_bp_systolic_value,
        diastolic_value AS latest_bp_diastolic_value,
        is_home_bp_event AS latest_bp_is_home,
        is_abpm_bp_event AS latest_bp_is_abpm
    FROM {{ ref('int_blood_pressure_latest') }}
),

register_logic AS (
    SELECT
        p.person_id,
        
        -- Age restriction: ≥18 years for HTN register
        CASE WHEN age.age >= 18 THEN TRUE ELSE FALSE END AS meets_age_criteria,
        
        -- Diagnosis component
        COALESCE(diag.has_active_htn_diagnosis, FALSE) AS has_active_diagnosis,
        
        -- Final register inclusion: Age + Active diagnosis required
        CASE
            WHEN age.age >= 18
                AND diag.has_active_htn_diagnosis = TRUE
            THEN TRUE
            ELSE FALSE
        END AS is_on_htn_register,
        
        -- Clinical dates
        diag.earliest_htn_diagnosis_date,
        diag.latest_htn_diagnosis_date,
        diag.latest_htn_resolved_date,
        
        -- Latest BP data
        bp.latest_bp_date,
        bp.latest_bp_systolic_value,
        bp.latest_bp_diastolic_value,
        bp.latest_bp_is_home,
        bp.latest_bp_is_abpm,
        
        -- Complex clinical staging logic based on NICE guidelines with context-specific thresholds
        CASE
            WHEN bp.latest_bp_systolic_value IS NULL OR bp.latest_bp_diastolic_value IS NULL 
                THEN NULL -- Cannot stage without BP values
            -- Severe threshold (applies regardless of context)
            WHEN bp.latest_bp_systolic_value >= 180 OR bp.latest_bp_diastolic_value >= 120 
                THEN 'Severe HTN'
            -- Home/ABPM context - different thresholds per NICE guidance
            WHEN bp.latest_bp_is_home OR bp.latest_bp_is_abpm THEN
                CASE 
                    WHEN bp.latest_bp_systolic_value >= 150 OR bp.latest_bp_diastolic_value >= 95 
                        THEN 'Stage 2 HTN (Home/ABPM Threshold)'
                    WHEN bp.latest_bp_systolic_value >= 135 OR bp.latest_bp_diastolic_value >= 85 
                        THEN 'Stage 1 HTN (Home/ABPM Threshold)'
                    ELSE 'Normal (Home/ABPM Threshold)'
                END
            -- Clinic context - standard clinic thresholds
            ELSE
                CASE
                    WHEN bp.latest_bp_systolic_value >= 160 OR bp.latest_bp_diastolic_value >= 100 
                        THEN 'Stage 2 HTN (Clinic Threshold)'
                    WHEN bp.latest_bp_systolic_value >= 140 OR bp.latest_bp_diastolic_value >= 90 
                        THEN 'Stage 1 HTN (Clinic Threshold)'
                    ELSE 'Normal / High Normal (Clinic Threshold)'
                END
        END AS latest_bp_htn_stage,
        
        -- Traceability
        diag.all_diagnosis_concept_codes,
        diag.all_diagnosis_concept_displays,
        diag.all_source_cluster_ids,
        
        -- Person demographics
        age.age
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN hypertension_diagnoses diag ON p.person_id = diag.person_id
    LEFT JOIN latest_bp_data bp ON p.person_id = bp.person_id
)

-- Final selection: Only individuals with active HTN diagnosis
SELECT
    person_id,
    age,
    is_on_htn_register,
    
    -- Clinical diagnosis dates
    earliest_htn_diagnosis_date,
    latest_htn_diagnosis_date,
    latest_htn_resolved_date,
    
    -- Latest BP data and staging
    latest_bp_date,
    latest_bp_systolic_value,
    latest_bp_diastolic_value,
    latest_bp_is_home,
    latest_bp_is_abpm,
    latest_bp_htn_stage,
    
    -- Traceability for audit
    all_diagnosis_concept_codes,
    all_diagnosis_concept_displays,
    all_source_cluster_ids,
    
    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis
FROM register_logic
WHERE is_on_htn_register = TRUE 