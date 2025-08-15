{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
**Frailty Register - Clinical Quality Measures**

Simple Register

Business Logic:
- Presence of frailty diagnosis (FRAILTY_DX) = on register
- Tracks latest frailty severity (mild, moderate, severe)

Note:
- There are no resolved codes for frailty (condition can fluctuate but doesn't "resolve")
- No specific age restrictions, though frailty is more common in elderly populations
- Latest severity assessment takes precedence for stratification

Clinical Context:
Used for frailty quality measures including:
- Comprehensive geriatric assessment planning
- Falls prevention and management
- Medication review and deprescribing
- Care coordination and support services
- Risk stratification for healthcare interventions
*/

WITH latest_severity AS (
    -- Get the latest frailty severity for each person
    SELECT 
        person_id,
        frailty_severity AS latest_frailty_severity
    FROM (
        SELECT 
            person_id,
            frailty_severity,
            ROW_NUMBER() OVER (
                PARTITION BY person_id 
                ORDER BY clinical_effective_date DESC, observation_id DESC
            ) AS rn
        FROM {{ ref('int_frailty_diagnoses_all') }}
        WHERE is_frailty_diagnosis_code = TRUE
    )
    WHERE rn = 1
),

frailty_diagnoses AS (
    SELECT
        fd.person_id,

        -- Person-level aggregation from observation-level data
        MIN(
            CASE
                WHEN is_frailty_diagnosis_code THEN clinical_effective_date
            END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE
                WHEN is_frailty_diagnosis_code THEN clinical_effective_date
            END
        ) AS latest_diagnosis_date,

        -- Register logic: active diagnosis required
        COALESCE(MAX(
            CASE
                WHEN is_frailty_diagnosis_code THEN clinical_effective_date
            END
        ) IS NOT NULL,
        FALSE) AS has_active_frailty_diagnosis,

        -- Count of frailty diagnoses (may indicate progression or reassessment)
        COUNT(CASE WHEN is_frailty_diagnosis_code THEN 1 END)
            AS total_frailty_diagnoses,

        -- Severity-specific counts
        COUNT(CASE WHEN frailty_severity = 'Mild' THEN 1 END) AS mild_frailty_count,
        COUNT(CASE WHEN frailty_severity = 'Moderate' THEN 1 END) AS moderate_frailty_count,
        COUNT(CASE WHEN frailty_severity = 'Severe' THEN 1 END) AS severe_frailty_count,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE WHEN is_frailty_diagnosis_code THEN concept_code END
        ) AS all_frailty_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_frailty_diagnosis_code THEN concept_display
            END
        ) AS all_frailty_concept_displays

    FROM {{ ref('int_frailty_diagnoses_all') }} fd
    GROUP BY fd.person_id
)

-- Final selection with person demographics
SELECT
    fd.person_id,
    age.age,
    TRUE AS is_on_register,
    fd.earliest_diagnosis_date,
    fd.latest_diagnosis_date,
    ls.latest_frailty_severity,
    fd.mild_frailty_count,
    fd.moderate_frailty_count,
    fd.severe_frailty_count,
    fd.total_frailty_diagnoses,
    fd.all_frailty_concept_codes,
    fd.all_frailty_concept_displays

FROM frailty_diagnoses AS fd
INNER JOIN latest_severity AS ls
    ON fd.person_id = ls.person_id
INNER JOIN {{ ref('dim_person') }} AS p
    ON fd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} AS age
    ON fd.person_id = age.person_id
WHERE fd.has_active_frailty_diagnosis = TRUE  -- Only include persons with active frailty diagnosis