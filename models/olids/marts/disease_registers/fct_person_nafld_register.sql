{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Non-Alcoholic Fatty Liver Disease (NAFLD) register fact table - one row per person.
Applies NAFLD register inclusion criteria.

Clinical Purpose:
- NAFLD diagnosis tracking and monitoring
- Liver health assessment
- Clinical register (NOT part of QOF)

Business Logic:
- Any NAFLD diagnosis code (hardcoded SNOMED concepts)
- No age restrictions
- No resolution codes

⚠️ TODO: Update with proper cluster ID once NAFLD_COD becomes available in REFERENCE.

Includes only active patients as per standard population requirements.
This table provides one row per person for analytical use.
*/

WITH nafld_diagnoses AS (
    SELECT
        person_id,

        -- Register inclusion dates
        MIN(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE WHEN is_diagnosis_code THEN clinical_effective_date END
        ) AS latest_diagnosis_date,

        -- Episode counts
        COUNT(CASE WHEN is_diagnosis_code THEN 1 END)
            AS total_nafld_episodes,

        -- Concept code arrays for traceability
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_code END
        )
            AS nafld_diagnosis_codes,
        ARRAY_AGG(
            DISTINCT CASE WHEN is_diagnosis_code THEN concept_display END
        )
            AS nafld_diagnosis_displays

    FROM {{ ref('int_nafld_diagnoses_all') }}
    GROUP BY person_id
),

register_inclusion AS (
    SELECT
        nd.*,

        -- Simple register logic: Include if has diagnosis
        COALESCE(earliest_diagnosis_date IS NOT NULL, FALSE) AS is_on_register,

        -- Clinical interpretation
        CASE
            WHEN earliest_diagnosis_date IS NOT NULL
                THEN 'Active NAFLD diagnosis'
            ELSE 'No NAFLD diagnosis'
        END AS nafld_status,


    FROM nafld_diagnoses AS nd
)

SELECT
    ri.person_id,
    ri.is_on_register,
    ri.nafld_status,
    ri.earliest_diagnosis_date,
    ri.latest_diagnosis_date,
    ri.total_nafld_episodes,
    ri.nafld_diagnosis_codes,
    ri.nafld_diagnosis_displays

FROM register_inclusion AS ri
INNER JOIN {{ ref('dim_person_active_patients') }} AS ap
    ON ri.person_id = ap.person_id
WHERE ri.is_on_register = TRUE

ORDER BY ri.earliest_diagnosis_date DESC, ri.person_id ASC
