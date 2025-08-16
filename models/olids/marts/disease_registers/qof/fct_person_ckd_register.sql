{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Chronic Kidney Disease (CKD) Register
-- Business Logic: Age ≥18 + Active CKD diagnosis (latest CKD_COD > latest CKDRES_COD OR no resolution recorded)
-- Lab data available separately in intermediate tables for clinical monitoring

WITH ckd_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        MIN(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS earliest_diagnosis_date,
        MAX(CASE WHEN is_diagnosis_code THEN clinical_effective_date END)
            AS latest_diagnosis_date,
        MAX(CASE WHEN is_resolved_code THEN clinical_effective_date END)
            AS latest_resolved_date,

        -- QOF register logic: active diagnosis required
        COALESCE(MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) IS NOT NULL
        AND (
            MAX(
                CASE WHEN is_resolved_code THEN clinical_effective_date END
            ) IS NULL
            OR MAX(
                CASE WHEN is_diagnosis_code THEN clinical_effective_date END
            )
            > MAX(
                CASE WHEN is_resolved_code THEN clinical_effective_date END
            )
        ), FALSE) AS has_active_ckd_diagnosis,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_ckd_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_resolved_code THEN concept_code END
        ) AS all_resolved_concept_codes

    FROM {{ ref('int_ckd_diagnoses_all') }}
    GROUP BY person_id
),

register_logic AS (
    SELECT
        p.person_id,

        -- Age restriction: ≥18 years for CKD register
        diag.earliest_diagnosis_date,

        -- Diagnosis component (only requirement)
        diag.latest_diagnosis_date,

        -- Final register inclusion: Age + Active diagnosis required
        diag.latest_resolved_date,

        -- Clinical dates
        diag.all_ckd_concept_codes,
        diag.all_resolved_concept_codes,
        age.age,

        -- Traceability
        COALESCE(age.age >= 18, FALSE) AS meets_age_criteria,
        COALESCE(diag.has_active_ckd_diagnosis, FALSE) AS has_active_diagnosis,

        -- Person demographics
        COALESCE(
            age.age >= 18
            AND diag.has_active_ckd_diagnosis = TRUE, FALSE
        ) AS is_on_register
    FROM {{ ref('dim_person') }} AS p
    INNER JOIN {{ ref('dim_person_age') }} AS age ON p.person_id = age.person_id
    LEFT JOIN ckd_diagnoses AS diag ON p.person_id = diag.person_id
)

-- Final selection: Only individuals with active CKD diagnosis
SELECT
    person_id,
    age,
    is_on_register,

    -- Clinical diagnosis dates
    earliest_diagnosis_date,
    latest_diagnosis_date,
    latest_resolved_date,

    -- Traceability for audit
    all_ckd_concept_codes,
    all_resolved_concept_codes,

    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis
FROM register_logic
WHERE is_on_register = TRUE
