{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

WITH twelve_months_ago AS (
    SELECT DATEADD(MONTH, -12, CURRENT_DATE()) AS twelve_months_ago
),

diabetes_register AS (
    -- Base population: people on the diabetes register (one row per person)
    SELECT person_id
    FROM {{ ref('fct_person_diabetes_register') }}
    -- No WHERE clause needed - this table already filters to people on the register
),

care_process_data AS (
    SELECT
        dr.person_id,

        -- HbA1c
        hba.clinical_effective_date AS latest_hba1c_date,
        hba.hba1c_value AS latest_hba1c_value,
        bp.clinical_effective_date AS latest_bp_date,

        -- Blood Pressure
        chol.clinical_effective_date AS latest_cholesterol_date,
        cre.clinical_effective_date AS latest_creatinine_date,

        -- Cholesterol
        acr.clinical_effective_date AS latest_acr_date,
        fc.clinical_effective_date AS latest_foot_check_date,

        -- Serum Creatinine
        bmi.clinical_effective_date AS latest_bmi_date,
        smok.clinical_effective_date AS latest_smoking_date,

        -- Urine ACR
        COALESCE(
            hba.clinical_effective_date >= t.twelve_months_ago,
            FALSE
        ) AS hba1c_completed_in_last_12m,
        COALESCE(
            bp.clinical_effective_date >= t.twelve_months_ago,
            FALSE
        ) AS bp_completed_in_last_12m,

        -- Foot Check
        COALESCE(
            chol.clinical_effective_date >= t.twelve_months_ago,
            FALSE
        ) AS cholesterol_completed_in_last_12m,
        -- Foot check completed if within 12 months AND both feet checked OR one foot checked and other absent/amputated AND not declined/unsuitable
        COALESCE(
            cre.clinical_effective_date >= t.twelve_months_ago,
            FALSE
        ) AS creatinine_completed_in_last_12m,

        -- BMI
        COALESCE(
            acr.clinical_effective_date >= t.twelve_months_ago,
            FALSE
        ) AS acr_completed_in_last_12m,
        COALESCE(
            fc.clinical_effective_date IS NOT NULL
            AND fc.clinical_effective_date >= t.twelve_months_ago
            AND (
                fc.both_feet_checked
                OR (
                    fc.left_foot_checked
                    AND (fc.right_foot_absent OR fc.right_foot_amputated)
                )
                OR (
                    fc.right_foot_checked
                    AND (fc.left_foot_absent OR fc.left_foot_amputated)
                )
            )
            AND NOT (fc.is_unsuitable OR fc.is_declined),
            FALSE
        ) AS foot_check_completed_in_last_12m,

        -- Smoking
        COALESCE(
            bmi.clinical_effective_date >= t.twelve_months_ago,
            FALSE
        ) AS bmi_completed_in_last_12m,
        COALESCE(
            smok.clinical_effective_date >= t.twelve_months_ago,
            FALSE
        ) AS smoking_completed_in_last_12m

    FROM diabetes_register AS dr
    CROSS JOIN twelve_months_ago AS t
    LEFT JOIN {{ ref('int_hba1c_latest') }} AS hba
        ON dr.person_id = hba.person_id
    LEFT JOIN {{ ref('int_blood_pressure_latest') }} AS bp
        ON dr.person_id = bp.person_id
    LEFT JOIN {{ ref('int_cholesterol_latest') }} AS chol
        ON dr.person_id = chol.person_id
    LEFT JOIN {{ ref('int_creatinine_latest') }} AS cre
        ON dr.person_id = cre.person_id
    LEFT JOIN {{ ref('int_urine_acr_latest') }} AS acr
        ON dr.person_id = acr.person_id
    LEFT JOIN {{ ref('int_foot_examination_latest') }} AS fc
        ON dr.person_id = fc.person_id
    LEFT JOIN {{ ref('int_bmi_latest') }} AS bmi
        ON dr.person_id = bmi.person_id
    LEFT JOIN {{ ref('int_smoking_status_latest') }} AS smok
        ON dr.person_id = smok.person_id
)

SELECT
    person_id,

    -- Individual care process dates and completion flags
    latest_hba1c_date,
    hba1c_completed_in_last_12m,
    latest_hba1c_value,

    latest_bp_date,
    bp_completed_in_last_12m,

    latest_cholesterol_date,
    cholesterol_completed_in_last_12m,

    latest_creatinine_date,
    creatinine_completed_in_last_12m,

    latest_acr_date,
    acr_completed_in_last_12m,

    latest_foot_check_date,
    foot_check_completed_in_last_12m,

    latest_bmi_date,
    bmi_completed_in_last_12m,

    latest_smoking_date,
    smoking_completed_in_last_12m,

    -- Overall completion metrics
    (
        CASE WHEN hba1c_completed_in_last_12m THEN 1 ELSE 0 END
        + CASE WHEN bp_completed_in_last_12m THEN 1 ELSE 0 END
        + CASE WHEN cholesterol_completed_in_last_12m THEN 1 ELSE 0 END
        + CASE WHEN creatinine_completed_in_last_12m THEN 1 ELSE 0 END
        + CASE WHEN acr_completed_in_last_12m THEN 1 ELSE 0 END
        + CASE WHEN foot_check_completed_in_last_12m THEN 1 ELSE 0 END
        + CASE WHEN bmi_completed_in_last_12m THEN 1 ELSE 0 END
        + CASE WHEN smoking_completed_in_last_12m THEN 1 ELSE 0 END
    ) AS care_processes_completed,

    COALESCE((
        hba1c_completed_in_last_12m
        AND bp_completed_in_last_12m
        AND cholesterol_completed_in_last_12m
        AND creatinine_completed_in_last_12m
        AND acr_completed_in_last_12m
        AND foot_check_completed_in_last_12m
        AND bmi_completed_in_last_12m
        AND smoking_completed_in_last_12m
    ), FALSE) AS all_processes_completed

FROM care_process_data
ORDER BY person_id
