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
    ap.person_id,
    COALESCE(cpp.sk_patient_id, NULL) AS sk_patient_id,
    p.birth_year,
    p.birth_month,
    -- Calculate approximate birth date using exact midpoint of the month
    p.death_year,
    p.death_month,
    CASE
        WHEN p.birth_year IS NOT NULL AND p.birth_month IS NOT NULL
            THEN DATEADD(
                DAY,
                FLOOR(
                    DAY(
                        LAST_DAY(
                            TO_DATE(
                                p.birth_year || '-' || p.birth_month || '-01'
                            )
                        )
                    )
                    / 2
                ),
                TO_DATE(p.birth_year || '-' || p.birth_month || '-01')
            )
    END AS birth_date_approx,
    -- Calculate approximate death date using exact midpoint of the month
    CASE
        WHEN p.death_year IS NOT NULL AND p.death_month IS NOT NULL
            THEN DATEADD(
                DAY,
                FLOOR(
                    DAY(
                        LAST_DAY(
                            TO_DATE(
                                p.death_year || '-' || p.death_month || '-01'
                            )
                        )
                    )
                    / 2
                ),
                TO_DATE(p.death_year || '-' || p.death_month || '-01')
            )
    END AS death_date_approx,
    p.death_year IS NOT NULL AS is_deceased,
    COALESCE(p.is_dummy_patient, FALSE) AS is_dummy_patient
FROM all_persons AS ap
LEFT JOIN current_patient_per_person AS cpp
    ON ap.person_id = cpp.person_id
LEFT JOIN {{ ref('stg_olids_patient') }} AS p
    ON cpp.patient_id = p.id
