{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Atrial Fibrillation Register
-- Business Logic: Active AF diagnosis (latest AFIB_COD > latest AFIBRES_COD OR no resolution recorded)
-- No age restrictions or medication validation requirements

WITH af_diagnoses AS (
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
        ), FALSE) AS has_active_af_diagnosis,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_af_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END
        ) AS all_af_concept_displays

    FROM {{ ref('int_atrial_fibrillation_diagnoses_all') }}
    GROUP BY person_id
),

register_logic AS (
    SELECT
        p.person_id,
        age.age,

        -- No age restrictions for AF register
        TRUE AS meets_age_criteria,

        -- Diagnosis component (only requirement)
        diag.earliest_diagnosis_date,

        -- Final register inclusion: Active diagnosis required
        diag.latest_diagnosis_date,

        -- Clinical dates
        diag.latest_resolved_date,
        diag.all_af_concept_codes,
        diag.all_af_concept_displays,

        -- Traceability
        COALESCE(diag.has_active_af_diagnosis, FALSE) AS has_active_diagnosis,
        COALESCE(diag.has_active_af_diagnosis = TRUE, FALSE) AS is_on_register
    FROM {{ ref('dim_person') }} AS p
    INNER JOIN {{ ref('dim_person_age') }} AS age ON p.person_id = age.person_id
    LEFT JOIN af_diagnoses AS diag ON p.person_id = diag.person_id
)

-- Final selection: Only individuals with active AF diagnosis
SELECT
    person_id,
    age,
    is_on_register,

    -- Clinical diagnosis dates
    earliest_diagnosis_date,
    latest_diagnosis_date,
    latest_resolved_date,

    -- Traceability for audit
    all_af_concept_codes,
    all_af_concept_displays,

    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diagnosis
FROM register_logic
WHERE is_on_register = TRUE
