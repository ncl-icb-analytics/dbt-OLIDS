{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Diabetes Register (QOF Pattern 4: Type Classification Register)
-- Business Logic: Age ≥17 + Active diabetes diagnosis + Type classification (Type 1 vs Type 2 vs Unknown)
-- Type Hierarchy: Type 1 takes precedence if both types coded on same date

WITH diabetes_person_aggregates AS (
    SELECT
        person_id,

        -- General diabetes dates
        MIN(
            CASE WHEN is_general_diabetes_code THEN clinical_effective_date END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE WHEN is_general_diabetes_code THEN clinical_effective_date END
        ) AS latest_diagnosis_date,

        -- Type-specific dates
        MIN(CASE WHEN is_type1_diabetes_code THEN clinical_effective_date END)
            AS earliest_type1_date,
        MAX(CASE WHEN is_type1_diabetes_code THEN clinical_effective_date END)
            AS latest_type1_date,
        MIN(CASE WHEN is_type2_diabetes_code THEN clinical_effective_date END)
            AS earliest_type2_date,
        MAX(CASE WHEN is_type2_diabetes_code THEN clinical_effective_date END)
            AS latest_type2_date,

        -- Resolution dates
        MIN(
            CASE WHEN is_diabetes_resolved_code THEN clinical_effective_date END
        ) AS earliest_resolved_date,
        MAX(
            CASE WHEN is_diabetes_resolved_code THEN clinical_effective_date END
        ) AS latest_resolved_date,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_general_diabetes_code THEN concept_code END
        ) AS all_diabetes_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_general_diabetes_code THEN concept_display END
        ) AS all_diabetes_concept_displays,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_type1_diabetes_code THEN concept_code END
        ) AS all_type1_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_type2_diabetes_code THEN concept_code END
        ) AS all_type2_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diabetes_resolved_code THEN concept_code END
        ) AS all_resolved_concept_codes

    FROM {{ ref('int_diabetes_diagnoses_all') }}
    GROUP BY person_id
),

register_logic AS (
    SELECT
        p.person_id,

        -- Age restriction: ≥17 years for diabetes register
        diag.earliest_diagnosis_date,

        -- QOF register logic: active diabetes diagnosis required
        diag.latest_diagnosis_date,

        -- Final register inclusion: Age ≥17 + Active diabetes
        diag.earliest_type1_date,

        -- Type classification logic (only for those on register, following legacy logic)
        diag.latest_type1_date,

        -- Clinical dates
        diag.earliest_type2_date,
        diag.latest_type2_date,
        diag.earliest_resolved_date,
        diag.latest_resolved_date,
        diag.all_diabetes_concept_codes,
        diag.all_diabetes_concept_displays,
        diag.all_type1_concept_codes,
        diag.all_type2_concept_codes,

        -- Traceability
        diag.all_resolved_concept_codes,
        age.age,
        COALESCE(age.age >= 17, FALSE) AS meets_age_criteria,
        CASE
            WHEN diag.latest_resolved_date IS NULL THEN TRUE -- Never resolved
            WHEN diag.latest_diagnosis_date > diag.latest_resolved_date THEN TRUE -- Re-diagnosed after resolution
            ELSE FALSE -- Currently resolved
        END AS has_active_diabetes_diagnosis,
        COALESCE(
            age.age >= 17
            AND diag.earliest_diagnosis_date IS NOT NULL -- Has diabetes diagnosis
            AND (
                diag.latest_resolved_date IS NULL -- Never resolved
                OR diag.latest_diagnosis_date > diag.latest_resolved_date -- Re-diagnosed after resolution
            ), FALSE
        ) AS is_on_register,

        -- Person demographics
        CASE
            WHEN NOT is_on_register THEN NULL -- Not applicable if not on register
            -- Type 1 precedence: Latest Type 1 >= Latest Type 2 (or no Type 2)
            WHEN
                diag.latest_type1_date IS NOT NULL
                AND (
                    diag.latest_type2_date IS NULL
                    OR diag.latest_type1_date >= diag.latest_type2_date
                )
                THEN 'Type 1'
            -- Type 2: Latest Type 2 > Latest Type 1 (or no Type 1)
            WHEN
                diag.latest_type2_date IS NOT NULL
                AND (
                    diag.latest_type1_date IS NULL
                    OR diag.latest_type2_date > diag.latest_type1_date
                )
                THEN 'Type 2'
            -- Unknown: On register but no specific type codes
            ELSE 'Unknown'
        END AS diabetes_type
    FROM {{ ref('dim_person') }} AS p
    INNER JOIN {{ ref('dim_person_age') }} AS age ON p.person_id = age.person_id
    LEFT JOIN diabetes_person_aggregates AS diag ON p.person_id = diag.person_id
)

-- Final selection: Only individuals on diabetes register
SELECT
    person_id,
    age,
    is_on_register,
    diabetes_type,

    -- Clinical diagnosis dates
    earliest_diagnosis_date,
    latest_diagnosis_date,
    latest_resolved_date,

    -- Type-specific dates for clinical audit
    earliest_type1_date,
    latest_type1_date,
    earliest_type2_date,
    latest_type2_date,

    -- Traceability for audit
    all_diabetes_concept_codes,
    all_diabetes_concept_displays,
    all_type1_concept_codes,
    all_type2_concept_codes,
    all_resolved_concept_codes,

    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_diabetes_diagnosis
FROM register_logic
WHERE is_on_register = TRUE
