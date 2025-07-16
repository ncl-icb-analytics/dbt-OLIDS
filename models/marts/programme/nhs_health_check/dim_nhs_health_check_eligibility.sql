{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- NHS Health Check Status Fact Table
-- Business Logic: Eligibility and due status for NHS Health Checks

WITH nhs_health_check_eligibility AS (
    -- Basic eligibility: Age 40-74 without excluding conditions
    SELECT
        p.person_id,
        age.age,

        -- Check for excluding conditions (existing chronic conditions)
        coalesce(
            chd.person_id IS NOT NULL
            OR diabetes.person_id IS NOT NULL
            OR stroke.person_id IS NOT NULL
            OR ckd.person_id IS NOT NULL
            OR af.person_id IS NOT NULL
            OR hf.person_id IS NOT NULL
            OR fh.person_id IS NOT NULL, FALSE
        ) AS has_any_excluding_condition,

        -- Eligibility: age 40-74 without excluding conditions
        coalesce(
            age.age BETWEEN 40 AND 74
            AND NOT (
                chd.person_id IS NOT NULL
                OR diabetes.person_id IS NOT NULL
                OR stroke.person_id IS NOT NULL
                OR ckd.person_id IS NOT NULL
                OR af.person_id IS NOT NULL
                OR hf.person_id IS NOT NULL
                OR fh.person_id IS NOT NULL
            ), FALSE
        ) AS is_eligible_for_nhs_health_check

    FROM {{ ref('dim_person') }} AS p
    INNER JOIN {{ ref('dim_person_age') }} AS age ON p.person_id = age.person_id
    LEFT JOIN
        {{ ref('fct_person_chd_register') }} AS chd
        ON p.person_id = chd.person_id
    LEFT JOIN
        {{ ref('fct_person_diabetes_register') }} AS diabetes
        ON p.person_id = diabetes.person_id
    LEFT JOIN
        {{ ref('fct_person_stroke_tia_register') }} AS stroke
        ON p.person_id = stroke.person_id
    LEFT JOIN
        {{ ref('fct_person_ckd_register') }} AS ckd
        ON p.person_id = ckd.person_id
    LEFT JOIN
        {{ ref('fct_person_atrial_fibrillation_register') }} AS af
        ON p.person_id = af.person_id
    LEFT JOIN
        {{ ref('fct_person_heart_failure_register') }} AS hf
        ON p.person_id = hf.person_id
    LEFT JOIN
        {{ ref('fct_person_familial_hypercholesterolaemia_register') }} AS fh
        ON p.person_id = fh.person_id
)

SELECT
    elig.person_id,
    elig.age,
    elig.has_any_excluding_condition,
    elig.is_eligible_for_nhs_health_check,
    hc.clinical_effective_date AS latest_health_check_date,

    -- Person is due a health check if eligible AND (never had one OR last one > 5 years ago)
    coalesce(
        elig.is_eligible_for_nhs_health_check = TRUE
        AND (
            hc.clinical_effective_date IS NULL
            OR datediff(DAY, hc.clinical_effective_date, current_date())
            > 1825
        ), FALSE
    ) AS due_nhs_health_check

FROM nhs_health_check_eligibility AS elig
LEFT JOIN
    {{ ref('int_nhs_health_check_latest') }} AS hc
    ON elig.person_id = hc.person_id
