{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'birth_death'],
        cluster_by=['person_id'])
}}

-- Person Birth and Death Dimension Table
-- Core birth and death information for each person
-- Designed to be reused by other dimension tables for age calculations

SELECT DISTINCT
    pp.person_id,
    p.sk_patient_id,
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
FROM {{ ref('stg_olids_patient') }} AS p
INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
    ON p.id = pp.patient_id
WHERE p.birth_year IS NOT NULL AND p.birth_month IS NOT NULL
