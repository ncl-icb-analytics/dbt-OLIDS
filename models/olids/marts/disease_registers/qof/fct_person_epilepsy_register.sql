{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Epilepsy Register (QOF Pattern 3: Complex QOF Register with External Validation)
-- Business Logic: Age ≥18 + Active epilepsy diagnosis (latest EPIL_COD > latest EPILDRUG_COD) + Recent epilepsy medication (last 6 months)
-- External Validation: Requires medication confirmation to ensure active epilepsy management

WITH epilepsy_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        MIN(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS latest_diagnosis_date,
        MAX(
            CASE WHEN is_resolved_code THEN clinical_effective_date END
        ) AS latest_resolved_date,

        -- QOF register logic: active diagnosis required
        COALESCE(MAX(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) IS NOT NULL
        AND (
            MAX(
                CASE
                    WHEN is_resolved_code THEN clinical_effective_date
                END
            ) IS NULL
            OR MAX(
                CASE
                    WHEN is_diagnosis_code THEN clinical_effective_date
                END
            )
            > MAX(
                CASE
                    WHEN is_resolved_code THEN clinical_effective_date
                END
            )
        ), FALSE) AS has_active_epilepsy_diagnosis,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_epilepsy_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_epilepsy_concept_displays,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_resolved_code THEN concept_code END
        ) AS all_resolved_concept_codes

    FROM {{ ref('int_epilepsy_diagnoses_all') }}
    GROUP BY person_id
),

epilepsy_medications AS (
    SELECT
        person_id,
        MAX(order_date) AS latest_epilepsy_medication_date,
        MAX(order_medication_name) AS latest_epilepsy_medication_name,
        MAX(mapped_concept_code) AS latest_epilepsy_medication_concept_code,
        MAX(mapped_concept_display)
            AS latest_epilepsy_medication_concept_display,
        COUNT(*) AS recent_epilepsy_medication_count
    FROM {{ ref('int_epilepsy_medications_6m') }}
    GROUP BY person_id
),

register_logic AS (
    SELECT
        p.person_id,

        -- Age restriction: ≥18 years for epilepsy register
        diag.earliest_diagnosis_date,

        -- Diagnosis component
        diag.latest_diagnosis_date,

        -- Medication validation component (6-month lookback)
        diag.latest_resolved_date,

        -- Final register inclusion: ALL criteria must be met
        med.latest_epilepsy_medication_date,

        -- Clinical dates
        med.latest_epilepsy_medication_name,
        med.latest_epilepsy_medication_concept_code,
        med.latest_epilepsy_medication_concept_display,

        -- Medication details
        med.recent_epilepsy_medication_count,
        diag.all_epilepsy_concept_codes,
        diag.all_epilepsy_concept_displays,
        diag.all_resolved_concept_codes,
        age.age,

        -- Traceability
        COALESCE(age.age >= 18, FALSE) AS meets_age_criteria,
        COALESCE(diag.has_active_epilepsy_diagnosis, FALSE)
            AS has_active_diagnosis,
        COALESCE(
            med.latest_epilepsy_medication_date IS NOT NULL,
            FALSE
        ) AS has_recent_medication,

        -- Person demographics
        COALESCE(
            age.age >= 18
            AND diag.has_active_epilepsy_diagnosis = TRUE
            AND med.latest_epilepsy_medication_date IS NOT NULL,
            FALSE
        ) AS is_on_register
    FROM {{ ref('dim_person') }} AS p
    INNER JOIN {{ ref('dim_person_age') }} AS age ON p.person_id = age.person_id
    LEFT JOIN epilepsy_diagnoses AS diag ON p.person_id = diag.person_id
    LEFT JOIN epilepsy_medications AS med ON p.person_id = med.person_id
)

-- Final selection: Only individuals meeting ALL criteria for epilepsy register
SELECT
    person_id,
    age,
    is_on_register,

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
WHERE is_on_register = TRUE
