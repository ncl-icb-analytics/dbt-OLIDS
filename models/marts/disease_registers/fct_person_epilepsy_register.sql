{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- Epilepsy Register (QOF Pattern 3: Complex QOF Register with External Validation)
-- Business Logic: Age ≥18 + Active epilepsy diagnosis (latest EPIL_COD > latest EPILRES_COD) + Recent epilepsy medication (last 6 months)
-- External Validation: Requires medication confirmation to ensure active epilepsy management

WITH epilepsy_diagnoses AS (
    SELECT
        person_id,
        
        -- Person-level aggregation from observation-level data
        MIN(CASE WHEN is_epilepsy_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
        MAX(CASE WHEN is_epilepsy_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
        MAX(CASE WHEN is_epilepsy_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- QOF register logic: active diagnosis required
        CASE
            WHEN MAX(CASE WHEN is_epilepsy_diagnosis_code THEN clinical_effective_date END) IS NOT NULL 
                AND (MAX(CASE WHEN is_epilepsy_resolved_code THEN clinical_effective_date END) IS NULL 
                     OR MAX(CASE WHEN is_epilepsy_diagnosis_code THEN clinical_effective_date END) > 
                        MAX(CASE WHEN is_epilepsy_resolved_code THEN clinical_effective_date END))
            THEN TRUE
            ELSE FALSE
        END AS has_active_epilepsy_diagnosis,
        
        -- Traceability arrays
        ARRAY_AGG(DISTINCT CASE WHEN is_epilepsy_diagnosis_code THEN concept_code ELSE NULL END) AS all_epilepsy_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_epilepsy_diagnosis_code THEN concept_display ELSE NULL END) AS all_epilepsy_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_epilepsy_resolved_code THEN concept_code ELSE NULL END) AS all_resolved_concept_codes
        
    FROM {{ ref('int_epilepsy_diagnoses_all') }}
    GROUP BY person_id
),

epilepsy_medications AS (
    SELECT
        person_id,
        MAX(order_date) AS latest_epilepsy_medication_date,
        MAX(order_medication_name) AS latest_epilepsy_medication_name,
        MAX(mapped_concept_code) AS latest_epilepsy_medication_concept_code,
        MAX(mapped_concept_display) AS latest_epilepsy_medication_concept_display,
        COUNT(*) AS recent_epilepsy_medication_count
    FROM {{ ref('int_epilepsy_medications_6m') }}
    GROUP BY person_id
),

register_logic AS (
    SELECT
        p.person_id,
        
        -- Age restriction: ≥18 years for epilepsy register
        CASE WHEN age.age >= 18 THEN TRUE ELSE FALSE END AS meets_age_criteria,
        
        -- Diagnosis component
        COALESCE(diag.has_active_epilepsy_diagnosis, FALSE) AS has_active_diagnosis,
        
        -- Medication validation component (6-month lookback)
        CASE WHEN med.latest_epilepsy_medication_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_recent_medication,
        
        -- Final register inclusion: ALL criteria must be met
        CASE
            WHEN age.age >= 18
                AND diag.has_active_epilepsy_diagnosis = TRUE
                AND med.latest_epilepsy_medication_date IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_on_epilepsy_register,
        
        -- Clinical dates
        diag.earliest_diagnosis_date,
        diag.latest_diagnosis_date,
        diag.latest_resolved_date,
        
        -- Medication details
        med.latest_epilepsy_medication_date,
        med.latest_epilepsy_medication_name,
        med.latest_epilepsy_medication_concept_code,
        med.latest_epilepsy_medication_concept_display,
        med.recent_epilepsy_medication_count,
        
        -- Traceability
        diag.all_epilepsy_concept_codes,
        diag.all_epilepsy_concept_displays,
        diag.all_resolved_concept_codes,
        
        -- Person demographics
        age.age
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN epilepsy_diagnoses diag ON p.person_id = diag.person_id
    LEFT JOIN epilepsy_medications med ON p.person_id = med.person_id
)

-- Final selection: Only individuals meeting ALL criteria for epilepsy register
SELECT
    person_id,
    age,
    is_on_epilepsy_register,
    
    -- Clinical diagnosis dates
    earliest_diagnosis_date,
    latest_diagnosis_date,
    latest_resolved_date,
    
    -- Medication validation details  
    latest_epilepsy_medication_date,
    latest_epilepsy_medication_name,
    latest_epilepsy_medication_concept_code,
    latest_epilepsy_medication_concept_display,
    recent_epilepsy_medication_count,
    
    -- Traceability for audit
    all_epilepsy_concept_codes,
    all_epilepsy_concept_displays,
    all_resolved_concept_codes,
    
    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis,
    has_recent_medication
FROM register_logic
WHERE is_on_epilepsy_register = TRUE 