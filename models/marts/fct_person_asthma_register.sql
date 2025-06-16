{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- Asthma Register (QOF Pattern 3: Complex QOF Register with External Validation)
-- Business Logic: Age ≥6 + Active asthma diagnosis (latest AST_COD > latest ASTRES_COD) + Recent asthma medication (last 12 months)
-- External Validation: Requires medication confirmation to ensure active asthma management

WITH asthma_diagnoses AS (
    SELECT
        person_id,
        earliest_diagnosis_date AS earliest_asthma_diagnosis_date,
        latest_diagnosis_date AS latest_asthma_diagnosis_date,
        latest_resolved_date AS latest_asthma_resolved_date,
        
        -- QOF register logic: active diagnosis required
        CASE
            WHEN latest_diagnosis_date IS NOT NULL 
                AND (latest_resolved_date IS NULL OR latest_diagnosis_date > latest_resolved_date)
            THEN TRUE
            ELSE FALSE
        END AS has_active_asthma_diagnosis,
        
        -- Traceability arrays
        all_diagnosis_concept_codes,
        all_diagnosis_concept_displays,
        all_source_cluster_ids
    FROM {{ ref('int_asthma_diagnoses_all') }}
),

asthma_medications AS (
    SELECT
        person_id,
        latest_order_date AS latest_asthma_medication_date,
        latest_medication_name AS latest_asthma_medication_name,
        latest_medication_dose AS latest_asthma_medication_dose,
        latest_concept_code AS latest_asthma_medication_concept_code,
        latest_concept_display AS latest_asthma_medication_concept_display,
        recent_order_count AS recent_asthma_medication_count
    FROM {{ ref('int_asthma_medications_12m') }}
),

register_logic AS (
    SELECT
        p.person_id,
        
        -- Age restriction: ≥6 years for asthma register
        CASE WHEN age.age >= 6 THEN TRUE ELSE FALSE END AS meets_age_criteria,
        
        -- Diagnosis component
        COALESCE(diag.has_active_asthma_diagnosis, FALSE) AS has_active_diagnosis,
        
        -- Medication validation component
        CASE WHEN med.latest_asthma_medication_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_recent_medication,
        
        -- Final register inclusion: ALL criteria must be met
        CASE
            WHEN age.age >= 6
                AND diag.has_active_asthma_diagnosis = TRUE
                AND med.latest_asthma_medication_date IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_on_asthma_register,
        
        -- Clinical dates
        diag.earliest_asthma_diagnosis_date,
        diag.latest_asthma_diagnosis_date,
        diag.latest_asthma_resolved_date,
        
        -- Medication details
        med.latest_asthma_medication_date,
        med.latest_asthma_medication_name,
        med.latest_asthma_medication_dose,
        med.latest_asthma_medication_concept_code,
        med.latest_asthma_medication_concept_display,
        med.recent_asthma_medication_count,
        
        -- Traceability
        diag.all_diagnosis_concept_codes,
        diag.all_diagnosis_concept_displays,
        diag.all_source_cluster_ids,
        
        -- Person demographics
        age.age
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN asthma_diagnoses diag ON p.person_id = diag.person_id
    LEFT JOIN asthma_medications med ON p.person_id = med.person_id
)

-- Final selection: Only individuals meeting ALL criteria for asthma register
SELECT
    person_id,
    age,
    is_on_asthma_register,
    
    -- Clinical diagnosis dates
    earliest_asthma_diagnosis_date,
    latest_asthma_diagnosis_date,
    latest_asthma_resolved_date,
    
    -- Medication validation details
    latest_asthma_medication_date,
    latest_asthma_medication_name,
    latest_asthma_medication_dose,
    latest_asthma_medication_concept_code,
    latest_asthma_medication_concept_display,
    recent_asthma_medication_count,
    
    -- Traceability for audit
    all_diagnosis_concept_codes,
    all_diagnosis_concept_displays,
    all_source_cluster_ids,
    
    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis,
    has_recent_medication
FROM register_logic
WHERE is_on_asthma_register = TRUE 