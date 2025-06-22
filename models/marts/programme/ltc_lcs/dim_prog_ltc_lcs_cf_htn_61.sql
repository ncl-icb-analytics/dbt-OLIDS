{{ config(materialized='table') }}

-- HTN_61 case finding: Severe hypertension
-- Identifies patients with severe hypertension based on blood pressure thresholds

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
        CASE 
            WHEN NOT bp.is_home_bp_event AND NOT bp.is_abpm_bp_event THEN TRUE
            ELSE FALSE
        END AS is_clinic_bp,
        CASE 
            WHEN bp.is_home_bp_event OR bp.is_abpm_bp_event THEN TRUE
            ELSE FALSE
        END AS is_home_bp
    FROM {{ ref('int_blood_pressure_all') }} bp
    JOIN {{ ref('int_ltc_lcs_cf_base_population') }} base USING (person_id)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bp.person_id ORDER BY bp.clinical_effective_date DESC) = 1
),

eligible_patients AS (
    -- Patients with severe hypertension based on thresholds
    SELECT
        bp.person_id,
        base.age,
        bp.latest_bp_date,
        bp.systolic_value AS latest_bp_value,
        bp.latest_bp_type,
        bp.is_clinic_bp,
        bp.is_home_bp,
        CASE 
            WHEN bp.is_clinic_bp AND (bp.systolic_value >= 180 OR bp.diastolic_value >= 120) THEN TRUE
            WHEN bp.is_home_bp AND (bp.systolic_value >= 170 OR bp.diastolic_value >= 115) THEN TRUE
            ELSE FALSE
        END AS has_severe_hypertension
    FROM latest_bp bp
    JOIN {{ ref('int_ltc_lcs_cf_base_population') }} base USING (person_id)
    WHERE (
        (bp.is_clinic_bp AND (bp.systolic_value >= 180 OR bp.diastolic_value >= 120))
        OR 
        (bp.is_home_bp AND (bp.systolic_value >= 170 OR bp.diastolic_value >= 115))
    )
)

-- Final selection: patients with severe hypertension
SELECT
    ep.person_id,
    ep.age,
    TRUE AS has_severe_hypertension,  -- All patients in this cohort have severe hypertension
    ep.latest_bp_date,
    ep.latest_bp_value,
    ep.latest_bp_type,
    ep.is_clinic_bp,
    ep.is_home_bp
FROM eligible_patients ep 