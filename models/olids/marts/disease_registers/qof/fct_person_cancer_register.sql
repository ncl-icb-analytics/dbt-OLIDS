{{
    config(
        materialized='table')
}}

/*
Cancer Register - QOF Quality Measures
Tracks all patients with cancer diagnoses on/after April 1, 2003.

Simple Register Pattern with Date Filter:
- Presence of cancer diagnosis on/after 1 April 2003 = on register
- No resolution codes (cancer is permanent)
- No age restrictions
- Excludes non-melanotic skin cancers (handled in cluster definition)

QOF Business Rules:
1. Cancer diagnosis (CAN_COD) on/after 1 April 2003 qualifies for register
2. Cancer is considered a permanent condition - no resolution
3. Used for survivorship care and follow-up monitoring
4. Supports cancer care quality measures

*/

WITH cancer_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        MIN(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) AS latest_diagnosis_date,

        -- QOF register logic: cancer is permanent, any diagnosis since April 2003 qualifies
        COALESCE(MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) IS NOT NULL
        AND MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        )
        >= '2003-04-01', FALSE) AS has_active_cancer_diagnosis,

        -- Count of cancer episodes
        COUNT(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS total_cancer_episodes,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_cancer_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END
        ) AS all_cancer_concept_displays

    FROM {{ ref('int_cancer_diagnoses_all') }}
    GROUP BY person_id
),

-- Add person demographics matching legacy structure
final AS (
    SELECT
        cd.person_id,
        age.age,

        -- Register flag (always true after date filtering)
        cd.has_active_cancer_diagnosis AS is_on_register,

        -- Diagnosis dates
        cd.earliest_diagnosis_date,
        cd.latest_diagnosis_date,

        -- Code arrays for traceability
        cd.all_cancer_concept_codes,
        cd.all_cancer_concept_displays

    FROM cancer_diagnoses AS cd
    LEFT JOIN {{ ref('dim_person') }} AS p ON cd.person_id = p.person_id
    LEFT JOIN {{ ref('dim_person_age') }} AS age ON cd.person_id = age.person_id
    WHERE cd.has_active_cancer_diagnosis = TRUE  -- Only include persons with active cancer diagnosis
)

SELECT
    person_id,
    age,
    is_on_register,
    earliest_diagnosis_date,
    latest_diagnosis_date,
    all_cancer_concept_codes,
    all_cancer_concept_displays
FROM final
