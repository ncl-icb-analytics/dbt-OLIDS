{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}",
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'QOF Non-Diabetic Hyperglycaemia (NDH) Register - Patients with pre-diabetes for prevention interventions.

Key Inclusion Criteria:
• NDH diagnosis (NDH_COD) present - elevated glucose but not diabetic
• No age restrictions
• Excludes those with diabetes diagnosis

Purpose: QOF register for diabetes prevention, lifestyle interventions, and pre-diabetes monitoring to prevent progression to Type 2 diabetes.'"
        ]
    )
}}

/*
Non-Diabetic Hyperglycaemia (NDH) register fact table - one row per person.
Applies QOF NDH register inclusion criteria.

Clinical Purpose:
- QOF NDH register for diabetes prevention and intervention
- Pre-diabetes monitoring and lifestyle intervention
- Glucose metabolism disorder tracking
- Diabetes prevention pathway support

QOF Register Criteria (Complex Pattern):
- Age ≥18 years at diagnosis
- NDH/IGT/PRD diagnosis (NDH_COD, IGT_COD, PRD_COD)
- Complex diabetes exclusion: never had diabetes OR diabetes is resolved
- Important for diabetes prevention programmes

Includes only active patients as per QOF population requirements.
This table provides one row per person for analytical use.
*/

WITH ndh_diagnoses AS (
    SELECT
        person_id,

        -- Register inclusion dates for different NDH types
        MIN(CASE WHEN is_ndh_diagnosis_code THEN clinical_effective_date END)
            AS earliest_ndh_date,
        MAX(CASE WHEN is_ndh_diagnosis_code THEN clinical_effective_date END)
            AS latest_ndh_date,
        MIN(CASE WHEN is_igt_diagnosis_code THEN clinical_effective_date END)
            AS earliest_igt_date,
        MAX(CASE WHEN is_igt_diagnosis_code THEN clinical_effective_date END)
            AS latest_igt_date,
        MIN(
            CASE
                WHEN is_pre_diabetes_diagnosis_code THEN clinical_effective_date
            END
        ) AS earliest_prd_date,
        MAX(
            CASE
                WHEN is_pre_diabetes_diagnosis_code THEN clinical_effective_date
            END
        ) AS latest_prd_date,

        -- Overall NDH dates (any type)
        MIN(CASE WHEN is_any_ndh_type_code THEN clinical_effective_date END)
            AS earliest_diagnosis_date,
        MAX(CASE WHEN is_any_ndh_type_code THEN clinical_effective_date END)
            AS latest_diagnosis_date,

        -- Episode counts
        COUNT(CASE WHEN is_ndh_diagnosis_code THEN 1 END) AS total_ndh_episodes,
        COUNT(CASE WHEN is_igt_diagnosis_code THEN 1 END) AS total_igt_episodes,
        COUNT(CASE WHEN is_pre_diabetes_diagnosis_code THEN 1 END)
            AS total_prd_episodes,

        -- Subtype flags
        COALESCE(MIN(
            CASE WHEN is_ndh_diagnosis_code THEN clinical_effective_date END
        ) IS NOT NULL,
        FALSE) AS has_ndh_diagnosis,
        COALESCE(MIN(
            CASE WHEN is_igt_diagnosis_code THEN clinical_effective_date END
        ) IS NOT NULL,
        FALSE) AS has_igt_diagnosis,
        COALESCE(MIN(
            CASE
                WHEN is_pre_diabetes_diagnosis_code THEN clinical_effective_date
            END
        ) IS NOT NULL,
        FALSE) AS has_prd_diagnosis,

        -- Concept code arrays for traceability
        ARRAY_AGG(
            DISTINCT CASE WHEN is_ndh_diagnosis_code THEN concept_code END
        ) AS ndh_diagnosis_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_ndh_diagnosis_code THEN concept_display END
        ) AS ndh_diagnosis_displays,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_igt_diagnosis_code THEN concept_code END
        ) AS igt_diagnosis_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_igt_diagnosis_code THEN concept_display END
        ) AS igt_diagnosis_displays,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_pre_diabetes_diagnosis_code THEN concept_code
            END
        ) AS prd_diagnosis_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_pre_diabetes_diagnosis_code THEN concept_display
            END
        ) AS prd_diagnosis_displays,

        -- All observation IDs
        ARRAY_AGG(DISTINCT observation_id) AS all_observation_ids

    FROM {{ ref('int_ndh_diagnoses_all') }}
    GROUP BY person_id
),

diabetes_status AS (
    SELECT
        person_id,

        -- Diabetes history for exclusion logic
        MIN(
            CASE WHEN is_general_diabetes_code THEN clinical_effective_date END
        ) AS earliest_diabetes_diagnosis_date,
        MAX(
            CASE WHEN is_diabetes_resolved_code THEN clinical_effective_date END
        ) AS latest_diabetes_resolved_date,

        -- Diabetes flags
        COALESCE(MIN(
            CASE WHEN is_general_diabetes_code THEN clinical_effective_date END
        ) IS NOT NULL,
        FALSE) AS has_diabetes_diagnosis,
        COALESCE(
            MAX(
                CASE
                    WHEN is_diabetes_resolved_code THEN clinical_effective_date
                END
            )
            > MAX(
                CASE
                    WHEN is_general_diabetes_code THEN clinical_effective_date
                END
            ),
            FALSE
        ) AS is_diabetes_resolved

    FROM {{ ref('int_diabetes_diagnoses_all') }}
    GROUP BY person_id
),

register_inclusion AS (
    SELECT
        nd.*,
        ds.has_diabetes_diagnosis,
        ds.is_diabetes_resolved,
        ds.earliest_diabetes_diagnosis_date,
        ds.latest_diabetes_resolved_date,

        -- Age at first NDH diagnosis calculation using current age (approximation)
        CASE
            WHEN nd.earliest_diagnosis_date IS NOT NULL
                THEN
                    age.age
                    - DATEDIFF(YEAR, nd.earliest_diagnosis_date, CURRENT_DATE())
        END AS age_at_first_ndh_diagnosis,

        -- QOF register logic: Age ≥18 + NDH + (never diabetes OR diabetes resolved)
        COALESCE(
            nd.earliest_diagnosis_date IS NOT NULL
            AND (
                age.age
                - DATEDIFF(YEAR, nd.earliest_diagnosis_date, CURRENT_DATE())
            )
            >= 18
            AND (
                COALESCE(ds.has_diabetes_diagnosis, FALSE) = FALSE
                OR COALESCE(ds.is_diabetes_resolved, FALSE) = TRUE
            ), FALSE
        ) AS is_on_register,

        -- Clinical interpretation
        CASE
            WHEN
                nd.earliest_diagnosis_date IS NOT NULL
                AND (
                    age.age
                    - DATEDIFF(YEAR, nd.earliest_diagnosis_date, CURRENT_DATE())
                )
                >= 18
                AND (
                    COALESCE(ds.has_diabetes_diagnosis, FALSE) = FALSE
                    OR COALESCE(ds.is_diabetes_resolved, FALSE) = TRUE
                )
                THEN 'Active NDH - eligible for diabetes prevention'
            WHEN
                nd.earliest_diagnosis_date IS NOT NULL
                AND (
                    age.age
                    - DATEDIFF(YEAR, nd.earliest_diagnosis_date, CURRENT_DATE())
                )
                < 18
                THEN 'NDH diagnosis (age <18 - excluded from QOF)'
            WHEN
                nd.earliest_diagnosis_date IS NOT NULL
                AND COALESCE(ds.has_diabetes_diagnosis, FALSE) = TRUE
                AND COALESCE(ds.is_diabetes_resolved, FALSE) = FALSE
                THEN 'NDH diagnosis (excluded - unresolved diabetes)'
            ELSE 'No NDH diagnosis'
        END AS ndh_status,

        -- Days calculations
        CASE
            WHEN nd.earliest_diagnosis_date IS NOT NULL
                THEN DATEDIFF(DAY, nd.earliest_diagnosis_date, CURRENT_DATE())
        END AS days_since_first_ndh,

        CASE
            WHEN nd.latest_diagnosis_date IS NOT NULL
                THEN DATEDIFF(DAY, nd.latest_diagnosis_date, CURRENT_DATE())
        END AS days_since_latest_ndh

    FROM ndh_diagnoses AS nd
    INNER JOIN {{ ref('dim_person_active_patients') }} AS ap
        ON nd.person_id = ap.person_id
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON nd.person_id = age.person_id
    LEFT JOIN diabetes_status AS ds
        ON nd.person_id = ds.person_id
)

SELECT
    ri.person_id,
    ri.is_on_register,
    ri.ndh_status,
    ri.earliest_diagnosis_date,
    ri.latest_diagnosis_date,
    ri.earliest_ndh_date,
    ri.latest_ndh_date,
    ri.earliest_igt_date,
    ri.latest_igt_date,
    ri.earliest_prd_date,
    ri.latest_prd_date,
    ri.age_at_first_ndh_diagnosis,
    ri.total_ndh_episodes,
    ri.total_igt_episodes,
    ri.total_prd_episodes,
    ri.has_ndh_diagnosis,
    ri.has_igt_diagnosis,
    ri.has_prd_diagnosis,
    ri.has_diabetes_diagnosis,
    ri.is_diabetes_resolved,
    ri.earliest_diabetes_diagnosis_date,
    ri.latest_diabetes_resolved_date,
    ri.days_since_first_ndh,
    ri.days_since_latest_ndh,
    ri.ndh_diagnosis_codes,
    ri.ndh_diagnosis_displays,
    ri.igt_diagnosis_codes,
    ri.igt_diagnosis_displays,
    ri.prd_diagnosis_codes,
    ri.prd_diagnosis_displays,
    ri.all_observation_ids

FROM register_inclusion AS ri
WHERE ri.is_on_register = TRUE

ORDER BY ri.earliest_diagnosis_date DESC, ri.person_id ASC
