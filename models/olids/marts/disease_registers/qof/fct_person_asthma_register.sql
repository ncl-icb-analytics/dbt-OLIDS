{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Asthma Register (QOF Pattern 3: Complex QOF Register with External Validation)
-- Business Logic: Age ≥6 + Active asthma diagnosis (latest AST_COD > latest ASTRES_COD) + Recent asthma medication (last 12 months)
-- External Validation: Requires medication confirmation to ensure active asthma management

WITH asthma_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        MIN(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) AS latest_diagnosis_date,
        MAX(
            CASE WHEN is_resolved_code THEN clinical_effective_date END
        ) AS latest_resolved_date,

        -- QOF register logic: active diagnosis required
        COALESCE(MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
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
        ), FALSE) AS has_active_asthma_diagnosis,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_asthma_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END
        ) AS all_asthma_concept_displays,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_resolved_code THEN concept_code END
        ) AS all_resolved_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_resolved_code THEN concept_display END
        ) AS all_resolved_concept_displays

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

        -- Age restriction: ≥6 years for asthma register
        diag.earliest_diagnosis_date,

        -- Diagnosis component
        diag.latest_diagnosis_date,

        -- Medication validation component
        diag.latest_resolved_date,

        -- Final register inclusion: ALL criteria must be met
        med.latest_asthma_medication_date,

        -- Clinical dates
        med.latest_asthma_medication_name,
        med.latest_asthma_medication_concept_code,
        med.latest_asthma_medication_concept_display,

        -- Medication details
        med.recent_asthma_medication_count,
        diag.all_asthma_concept_codes,
        diag.all_asthma_concept_displays,
        diag.all_resolved_concept_codes,
        diag.all_resolved_concept_displays,

        -- Traceability
        age.age,
        COALESCE(age.age >= 6, FALSE) AS meets_age_criteria,
        COALESCE(diag.has_active_asthma_diagnosis, FALSE)
            AS has_active_diagnosis,
        COALESCE(
            med.latest_asthma_medication_date IS NOT NULL,
            FALSE
        ) AS has_recent_medication,

        -- Person demographics
        COALESCE(
            age.age >= 6
            AND diag.has_active_asthma_diagnosis = TRUE
            AND med.latest_asthma_medication_date IS NOT NULL,
            FALSE
        ) AS is_on_register
    FROM {{ ref('dim_person') }} AS p
    INNER JOIN {{ ref('dim_person_age') }} AS age ON p.person_id = age.person_id
    LEFT JOIN asthma_diagnoses AS diag ON p.person_id = diag.person_id
    LEFT JOIN asthma_medications AS med ON p.person_id = med.person_id
)

-- Final selection: Only individuals meeting ALL criteria for asthma register
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
