{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

WITH twelve_months_ago AS (
    SELECT DATEADD(MONTH, -12, CURRENT_DATE()) AS twelve_months_ago
)

SELECT
    eight.person_id,

    -- Copy all fields from 8 processes
    eight.latest_hba1c_date,
    eight.hba1c_completed_in_last_12m,
    eight.latest_hba1c_value,

    eight.latest_bp_date,
    eight.bp_completed_in_last_12m,

    eight.latest_cholesterol_date,
    eight.cholesterol_completed_in_last_12m,

    eight.latest_creatinine_date,
    eight.creatinine_completed_in_last_12m,

    eight.latest_acr_date,
    eight.acr_completed_in_last_12m,

    eight.latest_foot_check_date,
    eight.foot_check_completed_in_last_12m,

    eight.latest_bmi_date,
    eight.bmi_completed_in_last_12m,

    eight.latest_smoking_date,
    eight.smoking_completed_in_last_12m,

    -- Add retinal screening (9th process)
    ret.clinical_effective_date AS latest_retinal_screening_date,
    eight.care_processes_completed AS care_processes_8_completed,

    -- Overall completion metrics
    eight.all_processes_completed AS all_8_processes_completed,
    COALESCE(
        ret.clinical_effective_date >= t.twelve_months_ago,
        FALSE
    ) AS retinal_screening_completed_in_last_12m,
    eight.care_processes_completed
    + CASE
        WHEN ret.clinical_effective_date >= t.twelve_months_ago THEN 1 ELSE 0
    END AS care_processes_9_completed,
    COALESCE(
        eight.all_processes_completed
        AND ret.clinical_effective_date >= t.twelve_months_ago, FALSE
    ) AS all_9_processes_completed

FROM {{ ref('fct_person_diabetes_8_care_processes') }} AS eight
CROSS JOIN twelve_months_ago AS t
LEFT JOIN {{ ref('int_retinal_screening_latest') }} AS ret
    ON eight.person_id = ret.person_id

ORDER BY eight.person_id
