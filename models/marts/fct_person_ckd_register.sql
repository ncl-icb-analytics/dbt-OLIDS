{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- Chronic Kidney Disease (CKD) Register (QOF Pattern 2: Standard QOF Register with Resolution Logic)
-- Business Logic: Age ≥18 + Active CKD diagnosis (latest CKD_COD > latest CKDRES_COD OR no resolution recorded)
-- Lab data available separately in intermediate tables for clinical monitoring

WITH ckd_diagnoses AS (
    SELECT
        person_id,
        earliest_ckd_date AS earliest_ckd_diagnosis_date,
        latest_ckd_date AS latest_ckd_diagnosis_date,
        latest_resolved_date AS latest_ckd_resolved_date,
        
        -- QOF register logic: active diagnosis required (use existing logic)
        has_active_ckd_diagnosis,
        
        -- Traceability arrays
        all_ckd_concept_codes,
        all_resolved_concept_codes
    FROM {{ ref('int_ckd_diagnoses_all') }}
),

register_logic AS (
    SELECT
        p.person_id,
        
        -- Age restriction: ≥18 years for CKD register
        CASE WHEN age.age >= 18 THEN TRUE ELSE FALSE END AS meets_age_criteria,
        
        -- Diagnosis component (only requirement)
        COALESCE(diag.has_active_ckd_diagnosis, FALSE) AS has_active_diagnosis,
        
        -- Final register inclusion: Age + Active diagnosis required
        CASE
            WHEN age.age >= 18
                AND diag.has_active_ckd_diagnosis = TRUE
            THEN TRUE
            ELSE FALSE
        END AS is_on_ckd_register,
        
        -- Clinical dates
        diag.earliest_ckd_diagnosis_date,
        diag.latest_ckd_diagnosis_date,
        diag.latest_ckd_resolved_date,
        
        -- Traceability
        diag.all_ckd_concept_codes,
        diag.all_resolved_concept_codes,
        
        -- Person demographics
        age.age
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN ckd_diagnoses diag ON p.person_id = diag.person_id
)

-- Final selection: Only individuals with active CKD diagnosis
SELECT
    person_id,
    age,
    is_on_ckd_register,
    
    -- Clinical diagnosis dates
    earliest_ckd_diagnosis_date,
    latest_ckd_diagnosis_date,
    latest_ckd_resolved_date,
    
    -- Traceability for audit
    all_ckd_concept_codes,
    all_resolved_concept_codes,
    
    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis
FROM register_logic
WHERE is_on_ckd_register = TRUE 