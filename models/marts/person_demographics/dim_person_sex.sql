{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'sex'],
        cluster_by=['person_id'])
}}

-- Person Sex Dimension Table
-- Derives sex from gender concepts using dynamic concept lookups
-- Ensures one row per person by preferring a mapped patient with gender; falls back to current registration

WITH current_patient_per_person AS (
    -- Current registration per person (for fallback context)
    SELECT
        ipr.person_id,
        ipr.patient_id,
        ipr.sk_patient_id
    FROM {{ ref('int_patient_registrations') }} AS ipr
    WHERE ipr.is_current_registration = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ipr.person_id
        ORDER BY ipr.registration_start_date DESC, ipr.episode_of_care_id DESC
    ) = 1
),

best_patient_with_gender AS (
    -- Choose a single best patient per person: prefer one with a gender_concept_id
    SELECT
        pp.person_id,
        p.id AS patient_id,
        p.gender_concept_id,
        ROW_NUMBER() OVER (
            PARTITION BY pp.person_id
            ORDER BY CASE WHEN p.gender_concept_id IS NOT NULL THEN 1 ELSE 2 END,
                     p.id DESC
        ) AS rn
    FROM {{ ref('int_patient_person_unique') }} AS pp
    INNER JOIN {{ ref('stg_olids_patient') }} AS p
        ON pp.patient_id = p.id
),

all_persons AS (
    SELECT person_id FROM {{ ref('dim_person') }}
)

SELECT
    person_id,
    sex
FROM (
    SELECT
        ap.person_id,
        COALESCE(target_concept.display, source_concept.display, 'Unknown') AS sex,
        ROW_NUMBER() OVER (
            PARTITION BY ap.person_id 
            ORDER BY 
                CASE WHEN target_concept.display IS NOT NULL THEN 1 ELSE 2 END,
                target_concept.display,
                source_concept.display
        ) AS rn
    FROM all_persons AS ap
    LEFT JOIN best_patient_with_gender AS bpg
        ON ap.person_id = bpg.person_id AND bpg.rn = 1
    LEFT JOIN current_patient_per_person AS cpp
        ON ap.person_id = cpp.person_id
    -- Prefer gender from best mapped patient; fall back to current registration's patient
    LEFT JOIN {{ ref('stg_olids_patient') }} AS p_best
        ON bpg.patient_id = p_best.id
    LEFT JOIN {{ ref('stg_olids_patient') }} AS p_curr
        ON cpp.patient_id = p_curr.id
    {{ join_concept_display('COALESCE(p_best.gender_concept_id, p_curr.gender_concept_id)') }}
) ranked
WHERE rn = 1
