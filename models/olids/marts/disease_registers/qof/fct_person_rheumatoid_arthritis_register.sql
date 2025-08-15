{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}")
}}

/*
Rheumatoid Arthritis (RA) register fact table - one row per person.
Applies QOF RA register inclusion criteria.

Clinical Purpose:
- QOF RA register for musculoskeletal disease management
- Disease activity monitoring and treatment pathway identification
- Inflammatory arthritis care pathway

QOF Register Criteria:
- Any RA diagnosis code (RA_COD)
- Age ≥16 years at diagnosis (applied in this fact table)
- No resolution codes (simple diagnosis-based register)
- Lifelong condition register for ongoing disease management

Includes only active patients as per QOF population requirements.
This table provides one row per person for analytical use.
*/

WITH ra_diagnoses AS (
    SELECT
        person_id,

        -- Register inclusion dates
        MIN(CASE WHEN is_ra_diagnosis_code THEN clinical_effective_date END)
            AS earliest_diagnosis_date,
        MAX(CASE WHEN is_ra_diagnosis_code THEN clinical_effective_date END)
            AS latest_diagnosis_date,

        -- Episode counts
        COUNT(CASE WHEN is_ra_diagnosis_code THEN 1 END) AS total_ra_episodes,

        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_ra_diagnosis_code THEN concept_code END)
            AS ra_diagnosis_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_ra_diagnosis_code THEN concept_display END
        )
            AS ra_diagnosis_displays,

        -- Latest observation details
        ARRAY_AGG(DISTINCT observation_id) AS all_observation_ids

    FROM {{ ref('int_rheumatoid_arthritis_diagnoses_all') }}
    GROUP BY person_id
),

register_inclusion AS (
    SELECT
        rd.*,

        -- Age at first diagnosis calculation using current age (approximation)
        CASE
            WHEN earliest_diagnosis_date IS NOT NULL
                THEN
                    age.age
                    - DATEDIFF(YEAR, earliest_diagnosis_date, CURRENT_DATE())
        END AS age_at_first_ra_diagnosis,

        -- QOF register logic: Include if has diagnosis and estimated age ≥16 at first diagnosis
        COALESCE(
            earliest_diagnosis_date IS NOT NULL
            AND (
                age.age
                - DATEDIFF(YEAR, earliest_diagnosis_date, CURRENT_DATE())
            )
            >= 16, FALSE
        ) AS is_on_register,

        -- Clinical interpretation
        CASE
            WHEN
                earliest_diagnosis_date IS NOT NULL
                AND (
                    age.age
                    - DATEDIFF(YEAR, earliest_diagnosis_date, CURRENT_DATE())
                )
                >= 16
                THEN 'Active RA diagnosis (age ≥16)'
            WHEN
                earliest_diagnosis_date IS NOT NULL
                AND (
                    age.age
                    - DATEDIFF(YEAR, earliest_diagnosis_date, CURRENT_DATE())
                )
                < 16
                THEN 'RA diagnosis (age <16 - excluded from QOF)'
            ELSE 'No RA diagnosis'
        END AS ra_status,


    FROM ra_diagnoses AS rd
    INNER JOIN {{ ref('dim_person_active_patients') }} AS ap
        ON rd.person_id = ap.person_id
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON rd.person_id = age.person_id
)

SELECT
    ri.person_id,
    ri.is_on_register,
    ri.ra_status,
    ri.earliest_diagnosis_date,
    ri.latest_diagnosis_date,
    ri.age_at_first_ra_diagnosis,
    ri.total_ra_episodes,
    ri.ra_diagnosis_codes,
    ri.ra_diagnosis_displays,
    ri.all_observation_ids

FROM register_inclusion AS ri
WHERE ri.is_on_register = TRUE

ORDER BY ri.earliest_diagnosis_date DESC, ri.person_id ASC
