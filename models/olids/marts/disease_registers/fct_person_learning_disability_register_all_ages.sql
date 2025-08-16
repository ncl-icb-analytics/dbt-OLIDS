{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Learning Disability Register (All Ages)

Business Logic:
- Includes all persons with any learning disability diagnosis (LD_COD)
- No age restriction (unlike QOF which requires age ≥ 14)
- Based on observation-level input: {{ ref('int_learning_disability_diagnoses_all') }}

Outputs one row per person with earliest/latest diagnosis dates and traceability arrays.
*/

WITH learning_disability_diagnoses AS (
    SELECT
        person_id,

        MIN(
            CASE WHEN is_diagnosis_code
                 THEN clinical_effective_date END
        ) AS earliest_diagnosis_date,

        MAX(
            CASE WHEN is_diagnosis_code
                 THEN clinical_effective_date END
        ) AS latest_diagnosis_date,

        COALESCE(MAX(
            CASE WHEN is_diagnosis_code
                 THEN clinical_effective_date END
        ) IS NOT NULL, FALSE) AS has_active_ld_diagnosis,

        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_ld_concept_codes,

        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END
        ) AS all_ld_concept_displays

    FROM {{ ref('int_learning_disability_diagnoses_all') }}
    GROUP BY person_id
)

SELECT
    ld.person_id,
    ld.has_active_ld_diagnosis AS is_on_register,
    ld.earliest_diagnosis_date,
    ld.latest_diagnosis_date,
    ld.all_ld_concept_codes,
    ld.all_ld_concept_displays
FROM learning_disability_diagnoses AS ld
WHERE ld.has_active_ld_diagnosis = TRUE
ORDER BY person_id


