{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}")
}}

/*
Familial Hypercholesterolaemia (FH) register fact table - one row per person.
Applies clinical FH register inclusion criteria.

Clinical Purpose:
- Clinical FH register for genetic cardiovascular risk management
- Clinical register (NOT part of QOF)
- Familial hypercholesterolaemia cascade screening
- High-intensity statin therapy monitoring
- Family screening pathway identification

Register Criteria:
- Any FH diagnosis code (FHYP_COD)
- Age ≥20 years (applied in this fact table)
- No resolution codes (genetic condition)
- Important for cascade family screening programmes

Includes only active patients as per standard population requirements.
This table provides one row per person for analytical use.
*/

WITH fh_diagnoses AS (
    SELECT
        person_id,

        -- Register inclusion dates
        MIN(CASE WHEN is_fh_diagnosis_code THEN clinical_effective_date END)
            AS earliest_diagnosis_date,
        MAX(CASE WHEN is_fh_diagnosis_code THEN clinical_effective_date END)
            AS latest_diagnosis_date,

        -- Episode counts
        COUNT(CASE WHEN is_fh_diagnosis_code THEN 1 END) AS total_fh_episodes,

        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_fh_diagnosis_code THEN concept_code END)
            AS fh_diagnosis_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_fh_diagnosis_code THEN concept_display END
        )
            AS fh_diagnosis_displays,

        -- Latest observation details
        ARRAY_AGG(DISTINCT observation_id) AS all_observation_ids

    FROM {{ ref('int_familial_hypercholesterolaemia_diagnoses_all') }}
    GROUP BY person_id
),

register_inclusion AS (
    SELECT
        fd.*,

        -- Age at first diagnosis calculation using current age (approximation)
        CASE
            WHEN earliest_diagnosis_date IS NOT NULL
                THEN
                    age.age
                    - DATEDIFF(YEAR, earliest_diagnosis_date, CURRENT_DATE())
        END AS age_at_first_fh_diagnosis,

        -- Register logic: Include if has diagnosis and estimated age ≥20 at first diagnosis
        COALESCE(
            earliest_diagnosis_date IS NOT NULL
            AND (
                age.age
                - DATEDIFF(YEAR, earliest_diagnosis_date, CURRENT_DATE())
            )
            >= 20, FALSE
        ) AS is_on_register,

        -- Clinical interpretation
        CASE
            WHEN
                earliest_diagnosis_date IS NOT NULL
                AND (
                    age.age
                    - DATEDIFF(YEAR, earliest_diagnosis_date, CURRENT_DATE())
                )
                >= 20
                THEN 'Active FH diagnosis (age ≥20)'
            WHEN
                earliest_diagnosis_date IS NOT NULL
                AND (
                    age.age
                    - DATEDIFF(YEAR, earliest_diagnosis_date, CURRENT_DATE())
                )
                < 20
                THEN 'FH diagnosis (age <20 - excluded from register)'
            ELSE 'No FH diagnosis'
        END AS fh_status,

        -- Days calculations
        CASE
            WHEN earliest_diagnosis_date IS NOT NULL
                THEN DATEDIFF(DAY, earliest_diagnosis_date, CURRENT_DATE())
        END AS days_since_first_fh,

        CASE
            WHEN latest_diagnosis_date IS NOT NULL
                THEN DATEDIFF(DAY, latest_diagnosis_date, CURRENT_DATE())
        END AS days_since_latest_fh

    FROM fh_diagnoses AS fd
    INNER JOIN {{ ref('dim_person_active_patients') }} AS ap
        ON fd.person_id = ap.person_id
    INNER JOIN {{ ref('dim_person_age') }} AS age
        ON fd.person_id = age.person_id
)

SELECT
    ri.person_id,
    ri.is_on_register,
    ri.fh_status,
    ri.earliest_diagnosis_date,
    ri.latest_diagnosis_date,
    ri.age_at_first_fh_diagnosis,
    ri.total_fh_episodes,
    ri.days_since_first_fh,
    ri.days_since_latest_fh,
    ri.fh_diagnosis_codes,
    ri.fh_diagnosis_displays,
    ri.all_observation_ids

FROM register_inclusion AS ri
WHERE ri.is_on_register = TRUE

ORDER BY ri.earliest_diagnosis_date DESC, ri.person_id ASC
