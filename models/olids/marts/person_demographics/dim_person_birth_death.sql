{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'birth_death'],
        cluster_by=['person_id'])
}}

-- Person Birth and Death Dimension Table
-- Core birth and death information for each person
-- Designed to be reused by other dimension tables for age calculations

WITH current_patient_per_person AS (
    -- Current registration per person (for SK and current practice context)
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

persons_with_patients AS (
    -- Restrict to persons that have at least one mapped patient_id
    SELECT DISTINCT person_id
    FROM {{ ref('int_patient_person_unique') }}
),

patient_candidates AS (
    -- Candidate patient rows per person to source birth/death info
    SELECT
        pp.person_id,
        p.id AS patient_id,
        p.sk_patient_id,
        p.birth_year,
        p.birth_month,
        p.death_year,
        p.death_month,
        p.is_dummy_patient,
        CASE WHEN p.birth_year IS NOT NULL AND p.birth_month IS NOT NULL THEN 1 ELSE 0 END AS has_dob
    FROM {{ ref('int_patient_person_unique') }} AS pp
    INNER JOIN {{ ref('stg_olids_patient') }} AS p
        ON pp.patient_id = p.id
),

best_patient AS (
    -- Choose a single, best patient per person: prefer one with a DOB
    SELECT *
    FROM patient_candidates
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY person_id
        ORDER BY has_dob DESC, patient_id DESC
    ) = 1
)

SELECT
    ap.person_id,
    -- Prefer SK from current registration; otherwise from best mapped patient
    COALESCE(cpp.sk_patient_id, bp.sk_patient_id) AS sk_patient_id,
    bp.birth_year,
    bp.birth_month,
    -- Calculate approximate birth date using exact midpoint of the month
    bp.death_year,
    bp.death_month,
    CASE
        WHEN bp.birth_year IS NOT NULL AND bp.birth_month IS NOT NULL
            THEN DATEADD(
                DAY,
                FLOOR(
                    DAY(
                        LAST_DAY(
                            TO_DATE(
                                bp.birth_year || '-' || bp.birth_month || '-01'
                            )
                        )
                    )
                    / 2
                ),
                TO_DATE(bp.birth_year || '-' || bp.birth_month || '-01')
            )
    END AS birth_date_approx,
    -- Calculate approximate death date using exact midpoint of the month
    CASE
        WHEN bp.death_year IS NOT NULL AND bp.death_month IS NOT NULL
            THEN DATEADD(
                DAY,
                FLOOR(
                    DAY(
                        LAST_DAY(
                            TO_DATE(
                                bp.death_year || '-' || bp.death_month || '-01'
                            )
                        )
                    )
                    / 2
                ),
                TO_DATE(bp.death_year || '-' || bp.death_month || '-01')
            )
    END AS death_date_approx,
    bp.death_year IS NOT NULL AS is_deceased,
    COALESCE(bp.is_dummy_patient, FALSE) AS is_dummy_patient
FROM persons_with_patients AS ap
INNER JOIN best_patient AS bp
    ON ap.person_id = bp.person_id
LEFT JOIN current_patient_per_person AS cpp
    ON ap.person_id = cpp.person_id
