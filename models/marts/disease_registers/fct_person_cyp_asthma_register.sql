{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- CYP Asthma Register (Clinical Register - NOT part of QOF)
-- Business Logic: Age <18 + Active asthma diagnosis (latest AST_COD > latest ASTRES_COD) + Recent asthma medication (last 12 months)
-- External Validation: Requires medication confirmation for children/young people with asthma

WITH asthma_diagnoses AS (
    SELECT
        person_id,
        
        -- Person-level aggregation from observation-level data
        MIN(CASE WHEN is_asthma_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
        MAX(CASE WHEN is_asthma_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
        MAX(CASE WHEN is_asthma_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- QOF register logic: active diagnosis required
        CASE
            WHEN MAX(CASE WHEN is_asthma_diagnosis_code THEN clinical_effective_date END) IS NOT NULL 
                AND (MAX(CASE WHEN is_asthma_resolved_code THEN clinical_effective_date END) IS NULL 
                     OR MAX(CASE WHEN is_asthma_diagnosis_code THEN clinical_effective_date END) > 
                        MAX(CASE WHEN is_asthma_resolved_code THEN clinical_effective_date END))
            THEN TRUE
            ELSE FALSE
        END AS has_active_asthma_diagnosis,
        
        -- Traceability arrays
        ARRAY_AGG(DISTINCT CASE WHEN is_asthma_diagnosis_code THEN concept_code ELSE NULL END) AS all_asthma_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_asthma_diagnosis_code THEN concept_display ELSE NULL END) AS all_asthma_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_asthma_resolved_code THEN concept_code ELSE NULL END) AS all_resolved_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_asthma_resolved_code THEN concept_display ELSE NULL END) AS all_resolved_concept_displays
        
    FROM {{ ref('int_asthma_diagnoses_all') }}
    GROUP BY person_id
),

asthma_medications AS (
    SELECT
        person_id,
        MAX(order_date) AS latest_asthma_medication_date,
        MAX(order_medication_name) AS latest_asthma_medication_name,
        MAX(mapped_concept_code) AS latest_asthma_medication_concept_code,
        MAX(mapped_concept_display) AS latest_asthma_medication_concept_display,
        COUNT(*) AS recent_asthma_medication_count
    FROM {{ ref('int_asthma_medications_12m') }}
    GROUP BY person_id
),

register_logic AS (
    SELECT
        p.person_id,
        
        -- Age restriction: <18 years for CYP asthma register  
        CASE WHEN age.age < 18 THEN TRUE ELSE FALSE END AS meets_age_criteria,
        
        -- Diagnosis component
        COALESCE(diag.has_active_asthma_diagnosis, FALSE) AS has_active_diagnosis,
        
        -- Medication validation component
        CASE WHEN med.latest_asthma_medication_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_recent_medication,
        
        -- Final register inclusion: ALL criteria must be met
        CASE
            WHEN age.age < 18
                AND diag.has_active_asthma_diagnosis = TRUE
                AND med.latest_asthma_medication_date IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_on_register,
        
        -- Clinical dates
        diag.earliest_diagnosis_date,
        diag.latest_diagnosis_date,
        diag.latest_resolved_date,
        
        -- Medication details
        med.latest_asthma_medication_date,
        med.latest_asthma_medication_name,
        med.latest_asthma_medication_concept_code,
        med.latest_asthma_medication_concept_display,
        med.recent_asthma_medication_count,
        
        -- Traceability
        diag.all_asthma_concept_codes,
        diag.all_asthma_concept_displays,
        diag.all_resolved_concept_codes,
        diag.all_resolved_concept_displays,
        
        -- Person demographics
        age.age
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN asthma_diagnoses diag ON p.person_id = diag.person_id
    LEFT JOIN asthma_medications med ON p.person_id = med.person_id
)

-- Final selection: Only individuals meeting ALL criteria for CYP asthma register
SELECT
    person_id,
    age,
    is_on_register,
    
    -- Clinical diagnosis dates
    earliest_diagnosis_date,
    latest_diagnosis_date,
    latest_resolved_date,
    
    -- Medication validation details
    latest_asthma_medication_date,
    latest_asthma_medication_name,
    latest_asthma_medication_concept_code,
    latest_asthma_medication_concept_display,
    recent_asthma_medication_count,
    
    -- Traceability for audit
    all_asthma_concept_codes,
    all_asthma_concept_displays,
    all_resolved_concept_codes,
    all_resolved_concept_displays,
    
    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis,
    has_recent_medication
FROM register_logic
WHERE is_on_register = TRUE 