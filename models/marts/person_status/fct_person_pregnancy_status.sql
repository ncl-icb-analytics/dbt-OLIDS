{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Pregnancy Status Fact Table
-- Business Logic: Current pregnancy status based on recent pregnancy codes vs delivery/outcome codes
-- Population: Non-male individuals only

WITH pregnancy_aggregated AS (
    SELECT
        person_id,
        -- Get latest pregnancy code (indicates active pregnancy)
        MAX(
            CASE 
                WHEN is_pregnancy_code = TRUE 
                THEN clinical_effective_date 
            END
        ) AS latest_preg_date,
        -- Get latest delivery/outcome code (indicates pregnancy has ended)
        MAX(
            CASE
                WHEN is_delivery_outcome_code = TRUE
                THEN clinical_effective_date
            END
        ) AS latest_delivery_date,
        -- Only include codes from the last 9 months for traceability
        ARRAY_AGG(DISTINCT 
            CASE 
                WHEN clinical_effective_date >= DATEADD(MONTH, -9, CURRENT_DATE())
                THEN concept_code 
            END
        ) AS all_preg_concept_codes,
        ARRAY_AGG(DISTINCT 
            CASE 
                WHEN clinical_effective_date >= DATEADD(MONTH, -9, CURRENT_DATE())
                THEN concept_display 
            END
        ) AS all_preg_concept_displays,
        ARRAY_AGG(DISTINCT 
            CASE 
                WHEN clinical_effective_date >= DATEADD(MONTH, -9, CURRENT_DATE())
                THEN source_cluster_id 
            END
        ) AS all_preg_source_cluster_ids
    FROM {{ ref('int_pregnancy_observations_all') }}
    GROUP BY person_id
),

permanent_absence_risk AS (
    SELECT DISTINCT person_id
    FROM {{ ref('int_pregnancy_absence_risk_all') }}
),

pregnancy_status AS (
    SELECT
        p.person_id,

        -- Demographics (non-male only)
        age.age,
        sex.sex,

        -- Pregnancy logic: recent pregnancy code (last 9 months) after any delivery code
        preg.latest_preg_date AS latest_preg_cod_date,

        -- Pregnancy dates
        preg.latest_delivery_date AS latest_pregdel_cod_date,
        preg.all_preg_concept_codes,

        -- Child-bearing age flags
        preg.all_preg_concept_displays,
        preg.all_preg_source_cluster_ids,

        -- Permanent absence flag
        COALESCE(
            preg.latest_preg_date IS NOT NULL
            AND preg.latest_preg_date >= DATEADD(MONTH, -9, CURRENT_DATE())
            AND (
                preg.latest_delivery_date IS NULL
                OR preg.latest_preg_date > preg.latest_delivery_date
            ), FALSE
        ) AS is_currently_pregnant,

        -- Traceability
        COALESCE(age.age BETWEEN 12 AND 55, FALSE)
            AS is_child_bearing_age_12_55,
        COALESCE(age.age BETWEEN 0 AND 55, FALSE)
            AS is_child_bearing_age_0_55,
        COALESCE(perm.person_id IS NOT NULL, FALSE)
            AS has_permanent_absence_preg_risk_flag

    FROM {{ ref('dim_person') }} AS p
    INNER JOIN {{ ref('dim_person_age') }} AS age ON p.person_id = age.person_id
    INNER JOIN {{ ref('dim_person_sex') }} AS sex ON p.person_id = sex.person_id
    LEFT JOIN pregnancy_aggregated AS preg ON p.person_id = preg.person_id
    LEFT JOIN permanent_absence_risk AS perm ON p.person_id = perm.person_id
    WHERE sex.sex != 'Male' -- Only non-male individuals
)

SELECT
    person_id,
    age,
    sex,
    is_currently_pregnant,
    latest_preg_cod_date,
    latest_pregdel_cod_date,
    is_child_bearing_age_12_55,
    is_child_bearing_age_0_55,
    has_permanent_absence_preg_risk_flag,
    all_preg_concept_codes,
    all_preg_concept_displays,
    all_preg_source_cluster_ids
FROM pregnancy_status
WHERE is_currently_pregnant = TRUE -- Only include currently pregnant individuals
