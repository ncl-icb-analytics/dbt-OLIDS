{{ config(
    materialized='table') }}

-- HTN_62 case finding: Stage 2 hypertension (excluding severe hypertension HTN_61)
-- Identifies patients with stage 2 hypertension who are not in the severe hypertension category

WITH latest_bp AS (
    -- Get latest blood pressure reading for each person
    SELECT
        bp.person_id,
        bp.clinical_effective_date AS latest_bp_date,
        bp.systolic_value,
        bp.diastolic_value,
        bp.is_home_bp_event,
        bp.is_abpm_bp_event,
        CASE
            WHEN bp.is_abpm_bp_event THEN 'HYPERTENSION_BP_ABPM'
            WHEN bp.is_home_bp_event THEN 'HYPERTENSION_BP_HOME'
            ELSE 'HYPERTENSION_BP_CLINIC'
        END AS latest_bp_type,
        coalesce(
            NOT bp.is_home_bp_event AND NOT bp.is_abpm_bp_event,
            FALSE
        ) AS is_clinic_bp,
        coalesce(
            bp.is_home_bp_event OR bp.is_abpm_bp_event,
            FALSE
        ) AS is_home_bp
    FROM {{ ref('int_blood_pressure_all') }} AS bp
    INNER JOIN {{ ref('int_ltc_lcs_cf_base_population') }} USING (person_id)
    QUALIFY
        row_number()
            OVER (
                PARTITION BY bp.person_id
                ORDER BY bp.clinical_effective_date DESC
            )
        = 1
),

eligible_patients AS (
    -- Patients with stage 2 hypertension (excluding severe hypertension)
    SELECT
        bp.person_id,
        base.age,
        bp.latest_bp_date,
        bp.systolic_value AS latest_bp_value,
        bp.latest_bp_type,
        bp.is_clinic_bp,
        bp.is_home_bp,
        TRUE AS has_stage_2_hypertension
    FROM latest_bp AS bp
    INNER JOIN
        {{ ref('int_ltc_lcs_cf_base_population') }} AS base
        ON bp.person_id = base.person_id
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ ref('dim_ltc_lcs_cf_htn_61') }} AS htn61
        WHERE htn61.person_id = bp.person_id
    )
    AND (
        (
            bp.is_clinic_bp
            AND (bp.systolic_value >= 160 OR bp.diastolic_value >= 100)
        )
        OR
        (
            bp.is_home_bp
            AND (bp.systolic_value >= 150 OR bp.diastolic_value >= 95)
        )
    )
)

-- Final selection: patients with stage 2 hypertension (excluding severe)
SELECT
    ep.person_id,
    ep.age,
    ep.has_stage_2_hypertension,
    ep.latest_bp_date,
    ep.latest_bp_value,
    ep.latest_bp_type,
    ep.is_clinic_bp,
    ep.is_home_bp
FROM eligible_patients AS ep
