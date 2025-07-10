{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

WITH twelve_months_ago AS (
    SELECT DATEADD(MONTH, -12, CURRENT_DATE()) AS twelve_months_ago
),

diabetes_register AS (
    -- Base population: people on diabetes register with proper type classification
    SELECT
        person_id,
        diabetes_type
    FROM {{ ref('fct_person_diabetes_register') }}
    WHERE is_on_register = TRUE
)

SELECT
    dr.person_id,
    dr.diabetes_type,

    -- Enhanced HbA1c data presentation
    hba.clinical_effective_date AS latest_hba1c_date,
    hba.hba1c_value AS latest_hba1c_value,
    hba.result_unit_display AS hba1c_unit,
    hba.hba1c_category AS hba1c_clinical_category,
    bp.clinical_effective_date AS latest_bp_date,

    -- Blood pressure data from event-based structure
    bp.systolic_value AS latest_systolic,
    bp.diastolic_value AS latest_diastolic,
    chol.clinical_effective_date AS latest_chol_date,

    -- Cholesterol data
    chol.cholesterol_value AS latest_chol_value,
    CASE
        WHEN hba.is_ifcc THEN 'IFCC'
        WHEN hba.is_dcct THEN 'DCCT'
    END AS hba1c_type,

    -- Target achievement flags
    COALESCE(hba.meets_qof_target, FALSE) AS hba1c_in_target_range,

    -- BP target: <130/80 mmHg for diabetes patients (NICE guidelines)
    (
        bp.systolic_value IS NOT NULL AND bp.diastolic_value IS NOT NULL
        AND bp.systolic_value < 130 AND bp.diastolic_value < 80
    ) AS bp_in_target_range,

    (chol.cholesterol_value IS NOT NULL AND chol.cholesterol_value < 5)
        AS cholesterol_in_target_range,

    -- Overall target achievement
    (
        COALESCE(hba.meets_qof_target, FALSE)
        AND (
            bp.systolic_value IS NOT NULL AND bp.diastolic_value IS NOT NULL
            AND bp.systolic_value < 130 AND bp.diastolic_value < 80
        )
        AND (chol.cholesterol_value IS NOT NULL AND chol.cholesterol_value < 5)
    ) AS all_three_targets_met,

    -- Recency flags (within last 12 months)
    (
        hba.clinical_effective_date IS NOT NULL
        AND hba.clinical_effective_date >= t.twelve_months_ago
    ) AS hba1c_measured_in_last_12m,

    (
        bp.clinical_effective_date IS NOT NULL
        AND bp.clinical_effective_date >= t.twelve_months_ago
    ) AS bp_measured_in_last_12m,

    (
        chol.clinical_effective_date IS NOT NULL
        AND chol.clinical_effective_date >= t.twelve_months_ago
    ) AS cholesterol_measured_in_last_12m,

    -- Recent but out of range flags
    (
        hba.clinical_effective_date IS NOT NULL
        AND hba.clinical_effective_date >= t.twelve_months_ago
        AND NOT COALESCE(hba.meets_qof_target, FALSE)
    ) AS hba1c_recent_but_out_of_range,

    (
        bp.clinical_effective_date IS NOT NULL
        AND bp.clinical_effective_date >= t.twelve_months_ago
        AND NOT (
            bp.systolic_value IS NOT NULL AND bp.diastolic_value IS NOT NULL
            AND bp.systolic_value < 130 AND bp.diastolic_value < 80
        )
    ) AS bp_recent_but_out_of_range,

    (
        chol.clinical_effective_date IS NOT NULL
        AND chol.clinical_effective_date >= t.twelve_months_ago
        AND NOT (
            chol.cholesterol_value IS NOT NULL AND chol.cholesterol_value < 5
        )
    ) AS cholesterol_recent_but_out_of_range

FROM diabetes_register AS dr
CROSS JOIN twelve_months_ago AS t
LEFT JOIN {{ ref('int_hba1c_latest') }} AS hba
    ON dr.person_id = hba.person_id
LEFT JOIN {{ ref('int_blood_pressure_latest') }} AS bp
    ON dr.person_id = bp.person_id
LEFT JOIN {{ ref('int_cholesterol_latest') }} AS chol
    ON dr.person_id = chol.person_id

ORDER BY dr.person_id
