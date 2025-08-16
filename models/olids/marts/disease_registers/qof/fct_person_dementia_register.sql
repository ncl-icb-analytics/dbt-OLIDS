{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
**Dementia Register - QOF Mental Health Quality Measures**

Simple Register

Business Logic:
- Presence of dementia diagnosis (DEM_COD) = on register

Note:
- There are no resolved codes for dementia (dementia is permanent condition)
- There are no age restrictions for dementia register

Clinical Context:
Used for dementia quality measures including:
- Dementia care pathway monitoring
- Cognitive health assessment support
- Memory service referral tracking
- Early detection and ongoing care

*/

WITH dementia_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        NULL AS latest_resolved_date,
        MIN(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS latest_diagnosis_date,  -- Dementia has no resolved codes (permanent condition)

        -- QOF register logic: active diagnosis required (dementia is permanent, no resolved codes)
        COALESCE(MAX(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) IS NOT NULL,
        FALSE) AS has_active_dementia_diagnosis,

        -- Count of dementia diagnoses (may indicate progression or confirmation)
        COUNT(CASE WHEN is_diagnosis_code THEN 1 END)
            AS total_dementia_diagnoses,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        ) AS all_dementia_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_dementia_concept_displays,
        ARRAY_CONSTRUCT() AS all_resolved_concept_codes  -- No resolved codes for dementia

    FROM {{ ref('int_dementia_diagnoses_all') }}
    GROUP BY person_id
)

-- Final selection with person demographics
SELECT
    dd.person_id,
    age.age,
    TRUE AS is_on_register,
    dd.earliest_diagnosis_date,
    dd.latest_diagnosis_date,
    dd.all_dementia_concept_codes,
    dd.all_dementia_concept_displays

FROM dementia_diagnoses AS dd
INNER JOIN {{ ref('dim_person') }} AS p
    ON dd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON dd.person_id = age.person_id
WHERE dd.has_active_dementia_diagnosis = TRUE  -- Only include persons with active dementia diagnosis
