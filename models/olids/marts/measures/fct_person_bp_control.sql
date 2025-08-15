{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Blood Pressure Control Status (Matching Legacy Superior Design)
-- Applies patient-specific BP thresholds with priority ranking
-- Includes clinical context and risk-based timeliness assessment

WITH latest_bp AS (
    -- Get most recent BP event (paired systolic/diastolic) for each person
    SELECT
        person_id,
        clinical_effective_date,
        systolic_value,
        diastolic_value,
        is_home_bp_event,
        is_abpm_bp_event
    FROM {{ ref('int_blood_pressure_latest') }}
),

patient_characteristics AS (
    -- Gather key patient characteristics for BP threshold determination
    SELECT
        bp.person_id,
        bp.clinical_effective_date AS latest_bp_date,
        bp.systolic_value AS latest_systolic_value,
        bp.diastolic_value AS latest_diastolic_value,
        bp.is_home_bp_event,
        bp.is_abpm_bp_event,
        age.age,

        -- Diabetes status (Type 2 specifically for BP thresholds)
        dm.diabetes_type,
        acr.acr_value AS latest_acr_value,

        -- CKD status and latest ACR for threshold determination
        COALESCE(dm.is_on_register, FALSE) AS is_on_dm_register,
        COALESCE(ckd.person_id IS NOT NULL, FALSE) AS has_ckd,

        -- Hypertension diagnosis status
        COALESCE(htn.is_on_register, FALSE) AS is_diagnosed_htn

    FROM latest_bp AS bp
    INNER JOIN
        {{ ref('dim_person_age') }} AS age
        ON bp.person_id = age.person_id
    LEFT JOIN
        {{ ref('fct_person_diabetes_register') }} AS dm
        ON bp.person_id = dm.person_id
    LEFT JOIN
        {{ ref('fct_person_ckd_register') }} AS ckd
        ON bp.person_id = ckd.person_id
    LEFT JOIN
        {{ ref('fct_person_hypertension_register') }} AS htn
        ON bp.person_id = htn.person_id
    LEFT JOIN
        {{ ref('int_urine_acr_latest') }} AS acr
        ON bp.person_id = acr.person_id
),

ranked_thresholds AS (
    -- Apply BP thresholds with priority ranking (most stringent first)
    SELECT
        pc.*,
        thr.threshold_rule_id,
        thr.patient_group,
        thr.systolic_threshold,
        thr.diastolic_threshold,

        -- Priority ranking (lowest number = highest priority/most stringent)
        CASE thr.patient_group
            WHEN 'CKD_ACR_GE_70' THEN 1  -- Most stringent: CKD with high ACR
            WHEN 'T2DM' THEN 2           -- T2DM under 80
            WHEN 'CKD' THEN 3            -- General CKD under 80
            WHEN 'AGE_GE_80' THEN 4      -- Age 80+
            WHEN 'AGE_LT_80' THEN 5      -- Default: Age under 80
            ELSE 99
        END AS priority_rank

    FROM patient_characteristics AS pc
    INNER JOIN {{ ref('stg_reference_bp_thresholds') }} AS thr
        ON (
            -- Age-based thresholds (everyone gets one of these)
            (thr.patient_group = 'AGE_LT_80' AND pc.age < 80)
            OR (thr.patient_group = 'AGE_GE_80' AND pc.age >= 80)

            -- T2DM patients under 80 get more stringent threshold
            OR (
                thr.patient_group = 'T2DM' AND pc.is_on_dm_register
                AND pc.diabetes_type = 'Type 2' AND pc.age < 80
            )

            -- CKD patients under 80 (general CKD threshold)
            OR (thr.patient_group = 'CKD' AND pc.has_ckd AND pc.age < 80)

            -- CKD patients under 80 with high ACR (≥70) get most stringent threshold
            OR (
                thr.patient_group = 'CKD_ACR_GE_70' AND pc.has_ckd
                AND pc.latest_acr_value >= 70 AND pc.age < 80
            )
        )
    WHERE
        thr.threshold_type = 'TARGET_UPPER'
        AND thr.operator = 'BELOW'
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY pc.person_id ORDER BY priority_rank ASC)
        = 1
)

-- Final output: BP control status with applied thresholds and timeliness
SELECT
    rt.person_id,

    -- Latest BP event details
    rt.latest_bp_date,
    rt.latest_systolic_value,
    rt.latest_diastolic_value,
    rt.is_home_bp_event,
    rt.is_abpm_bp_event,

    -- Patient characteristics
    rt.age,
    rt.has_ckd,
    rt.is_diagnosed_htn,
    rt.latest_acr_value,
    rt.threshold_rule_id AS applied_threshold_rule_id,

    -- Applied threshold details
    rt.patient_group AS applied_patient_group,
    rt.systolic_threshold AS applied_systolic_threshold,
    rt.diastolic_threshold AS applied_diastolic_threshold,
    (rt.is_on_dm_register AND rt.diabetes_type = 'Type 2') AS has_t2dm,

    -- BP control status calculations
    COALESCE(
        rt.latest_systolic_value IS NOT NULL
        AND rt.latest_systolic_value < rt.systolic_threshold,
        FALSE
    ) AS is_systolic_controlled,

    COALESCE(
        rt.latest_diastolic_value IS NOT NULL
        AND rt.latest_diastolic_value < rt.diastolic_threshold,
        FALSE
    ) AS is_diastolic_controlled,

    -- Overall control: both systolic AND diastolic must be controlled
    COALESCE((
        rt.latest_systolic_value IS NOT NULL
        AND rt.latest_systolic_value < rt.systolic_threshold
    )
    AND (
        rt.latest_diastolic_value IS NOT NULL
        AND rt.latest_diastolic_value < rt.diastolic_threshold
    ), FALSE) AS is_overall_bp_controlled,

    -- BP reading timeliness assessment
    DATEDIFF(MONTH, rt.latest_bp_date, CURRENT_DATE())
        AS latest_bp_reading_age_months,

    -- Risk-based timeliness: higher risk = more frequent monitoring
    CASE
        -- Tier 1: High risk (T2DM OR CKD OR diagnosed HTN) - check within 12 months
        WHEN
            (
                (rt.is_on_dm_register AND rt.diabetes_type = 'Type 2')
                OR rt.has_ckd
                OR rt.is_diagnosed_htn
            )
            THEN
                COALESCE(
                    DATEDIFF(MONTH, rt.latest_bp_date, CURRENT_DATE())
                    <= 12,
                    FALSE
                )

        -- Tier 2: Medium risk (age ≥40, no high-risk conditions) - check within 24 months
        WHEN (
            NOT (rt.is_on_dm_register AND rt.diabetes_type = 'Type 2')
            AND NOT rt.has_ckd AND NOT rt.is_diagnosed_htn AND rt.age >= 40
        )
            THEN
                COALESCE(
                    DATEDIFF(MONTH, rt.latest_bp_date, CURRENT_DATE())
                    <= 24,
                    FALSE
                )

        -- Tier 3: Low risk (age <40, no high-risk conditions) - check within 60 months
        WHEN (
            NOT (rt.is_on_dm_register AND rt.diabetes_type = 'Type 2')
            AND NOT rt.has_ckd AND NOT rt.is_diagnosed_htn AND rt.age < 40
        )
            THEN
                COALESCE(
                    DATEDIFF(MONTH, rt.latest_bp_date, CURRENT_DATE())
                    <= 60,
                    FALSE
                )

        ELSE FALSE
    END AS is_latest_bp_within_recommended_interval

FROM ranked_thresholds AS rt
ORDER BY rt.person_id
