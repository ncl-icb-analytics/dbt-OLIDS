{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'sex'],
        cluster_by=['person_id'])
}}

-- Person Sex Dimension Table
-- Derives sex from gender concepts using dynamic concept lookups
-- Ensures one row per person by using the patient record from current GP registration

WITH current_patient_per_person AS (
    -- Get the patient_id for current GP registration for each person
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

-- Get all persons to ensure complete coverage
all_persons AS (
    SELECT person_id
    FROM {{ ref('dim_person') }}
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
    LEFT JOIN current_patient_per_person AS cpp
        ON ap.person_id = cpp.person_id
    LEFT JOIN {{ ref('stg_olids_patient') }} AS p
        ON cpp.patient_id = p.id
    {{ join_concept_display('p.gender_concept_id') }}
) ranked
WHERE rn = 1
