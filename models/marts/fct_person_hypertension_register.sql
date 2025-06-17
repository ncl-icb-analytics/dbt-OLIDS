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
        earliest_hypertension_date AS earliest_htn_diagnosis_date,
        latest_hypertension_date AS latest_htn_diagnosis_date,
        latest_resolved_date AS latest_htn_resolved_date,
        
        -- QOF register logic: active diagnosis required
        has_active_hypertension_diagnosis AS has_active_htn_diagnosis,
        
        -- Traceability arrays
        all_hypertension_concept_codes,
        all_hypertension_concept_displays,
        all_resolved_concept_codes,
        all_resolved_concept_displays
    FROM {{ ref('int_hypertension_diagnoses_all') }}
),

latest_bp_data AS (
    SELECT
        person_id,
        clinical_effective_date AS latest_bp_date,
        validated_value AS latest_bp_value,
        bp_type,
        -- Extract systolic/diastolic if available (note: simplified for now)
        CASE WHEN bp_type = 'Systolic' THEN validated_value ELSE NULL END AS systolic_value,
        CASE WHEN bp_type = 'Diastolic' THEN validated_value ELSE NULL END AS diastolic_value
    FROM {{ ref('int_blood_pressure_latest') }}
),

-- Get latest systolic and diastolic readings separately
latest_systolic AS (
    SELECT 
        person_id,
        MAX(CASE WHEN bp_type = 'Systolic' THEN validated_value END) AS latest_bp_systolic_value,
        MAX(CASE WHEN bp_type = 'Systolic' THEN clinical_effective_date END) AS latest_systolic_date
    FROM {{ ref('int_blood_pressure_all') }}
    WHERE bp_type = 'Systolic'
    GROUP BY person_id
),

latest_diastolic AS (
    SELECT 
        person_id,
        MAX(CASE WHEN bp_type = 'Diastolic' THEN validated_value END) AS latest_bp_diastolic_value,
        MAX(CASE WHEN bp_type = 'Diastolic' THEN clinical_effective_date END) AS latest_diastolic_date
    FROM {{ ref('int_blood_pressure_all') }}
    WHERE bp_type = 'Diastolic'
    GROUP BY person_id
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
        GREATEST(sys.latest_systolic_date, dias.latest_diastolic_date) AS latest_bp_date,
        sys.latest_bp_systolic_value,
        dias.latest_bp_diastolic_value,
        
        -- Complex clinical staging logic based on NICE guidelines with context-specific thresholds
        CASE
            WHEN sys.latest_bp_systolic_value IS NULL OR dias.latest_bp_diastolic_value IS NULL 
                THEN NULL -- Cannot stage without BP values
            -- Severe threshold (applies regardless of context)
            WHEN sys.latest_bp_systolic_value >= 180 OR dias.latest_bp_diastolic_value >= 120 
                THEN 'Severe HTN'
            -- Standard clinic thresholds (simplified - no home/ABPM context available)
            WHEN sys.latest_bp_systolic_value >= 160 OR dias.latest_bp_diastolic_value >= 100 
                THEN 'Stage 2 HTN'
            WHEN sys.latest_bp_systolic_value >= 140 OR dias.latest_bp_diastolic_value >= 90 
                THEN 'Stage 1 HTN'
            ELSE 'Normal / High Normal'
        END AS latest_bp_htn_stage,
        
        -- Traceability
        diag.all_hypertension_concept_codes,
        diag.all_hypertension_concept_displays,
        diag.all_resolved_concept_codes,
        diag.all_resolved_concept_displays,
        
        -- Person demographics
        age.age
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN hypertension_diagnoses diag ON p.person_id = diag.person_id
    LEFT JOIN latest_systolic sys ON p.person_id = sys.person_id
    LEFT JOIN latest_diastolic dias ON p.person_id = dias.person_id
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
    latest_bp_htn_stage,
    
    -- Traceability for audit
    all_hypertension_concept_codes,
    all_hypertension_concept_displays,
    all_resolved_concept_codes,
    all_resolved_concept_displays,
    
    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis
FROM register_logic
WHERE is_on_htn_register = TRUE 