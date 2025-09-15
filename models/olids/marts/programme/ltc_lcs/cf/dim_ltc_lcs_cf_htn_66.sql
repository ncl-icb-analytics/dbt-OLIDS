{{ config(
    materialized='table') }}

-- HTN_66 case finding: Stage 1 hypertension without cardiovascular risk factors
-- Identifies patients with stage 1 hypertension but NO cardiovascular risk factors
-- Implements age-based exclusion logic for patients aged ≥80

WITH latest_bp AS (
    -- Get latest blood pressure reading for each person
    SELECT
        bp.person_id,
        base.age,
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
    INNER JOIN {{ ref('int_ltc_lcs_cf_base_population') }} AS base 
        ON bp.person_id = base.person_id
    QUALIFY
        row_number()
            OVER (
                PARTITION BY bp.person_id
                ORDER BY bp.clinical_effective_date DESC
            )
        = 1
),

risk_factor_patients AS (
    -- Patients with cardiovascular risk factors (matching legacy logic)
    SELECT DISTINCT person_id
    FROM (
        -- Myocardial, cerebral, claudication from observations
        SELECT DISTINCT person_id
        FROM {{ ref('int_ltc_lcs_htn_observations') }}
        WHERE
            cluster_id IN (
                'HYPERTENSION_MYOCARDIAL',
                'HYPERTENSION_CEREBRAL',
                'HYPERTENSION_CLAUDICATION'
            )

        UNION

        -- CKD (eGFR < 60) from observations
        SELECT DISTINCT person_id
        FROM {{ ref('int_ltc_lcs_htn_observations') }}
        WHERE
            cluster_id = 'HYPERTENSION_EGFR'
            AND result_value < 60

        UNION

        -- Diabetes from observations
        SELECT DISTINCT person_id
        FROM {{ ref('int_ltc_lcs_htn_observations') }}
        WHERE cluster_id = 'HYPERTENSION_DIABETES'

        UNION

        -- BMI > 35 from observations
        SELECT DISTINCT person_id
        FROM {{ ref('int_ltc_lcs_htn_observations') }}
        WHERE
            cluster_id = 'HYPERTENSION_BMI'
            AND result_value > 35

        UNION

        -- Black or South Asian ethnicity
        SELECT DISTINCT person_id
        FROM {{ ref('int_ltc_lcs_ethnicity_observations') }}
        WHERE cluster_id = 'ETHNICITY_BAME'
    )
),

higher_priority_patients AS (
    -- Exclude patients from higher priority HTN models
    SELECT DISTINCT person_id
    FROM (
        SELECT person_id FROM {{ ref('dim_ltc_lcs_cf_htn_61') }}
        UNION
        SELECT person_id FROM {{ ref('dim_ltc_lcs_cf_htn_62') }}
        UNION
        SELECT person_id FROM {{ ref('dim_ltc_lcs_cf_htn_63') }}
        UNION
        SELECT person_id FROM {{ ref('dim_ltc_lcs_cf_htn_65') }}
    )
),

patients_without_risk_factors AS (
    -- Patients without any cardiovascular risk factors
    SELECT
        base.person_id,
        base.age,
        FALSE AS has_cardiovascular_risk_factors
    FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS base
    WHERE NOT EXISTS (
        SELECT 1 FROM risk_factor_patients AS rf
        WHERE rf.person_id = base.person_id
    )
    AND NOT EXISTS (
        SELECT 1 FROM higher_priority_patients AS hpp
        WHERE hpp.person_id = base.person_id
    )
),

eligible_patients AS (
    -- Patients without risk factors and stage 1 hypertension
    -- Implements age-based thresholds per legacy logic
    SELECT
        bp.person_id,
        bp.age,
        bp.latest_bp_date,
        bp.systolic_value,
        bp.diastolic_value,
        bp.latest_bp_type,
        bp.is_clinic_bp,
        bp.is_home_bp,
        FALSE AS has_cardiovascular_risk_factors,
        TRUE AS has_stage_1_hypertension_no_risk
    FROM latest_bp AS bp
    INNER JOIN
        patients_without_risk_factors AS pnrf
        ON bp.person_id = pnrf.person_id
    WHERE (
        -- For patients under 80: standard thresholds
        (
            bp.age < 80
            AND (
                (
                    bp.is_clinic_bp
                    AND (bp.systolic_value >= 140 OR bp.diastolic_value >= 90)
                )
                OR
                (
                    bp.is_home_bp
                    AND (bp.systolic_value >= 135 OR bp.diastolic_value >= 85)
                )
            )
        )
        OR
        -- For patients 80 and over: higher thresholds
        (
            bp.age >= 80
            AND (
                (
                    bp.is_clinic_bp
                    AND (bp.systolic_value >= 150 OR bp.diastolic_value >= 90)
                )
                OR
                (
                    bp.is_home_bp
                    AND (bp.systolic_value >= 145 OR bp.diastolic_value >= 85)
                )
            )
        )
    )
)

-- Final selection: patients without risk factors and stage 1 hypertension
SELECT
    ep.person_id,
    ep.age,
    ep.has_cardiovascular_risk_factors,
    ep.has_stage_1_hypertension_no_risk,
    ep.latest_bp_date,
    ep.systolic_value AS latest_systolic_value,
    ep.diastolic_value AS latest_diastolic_value,
    ep.latest_bp_type,
    ep.is_clinic_bp,
    ep.is_home_bp,
    -- Add threshold info for transparency
    CASE
        WHEN ep.age >= 80 AND ep.is_clinic_bp THEN '≥150/90'
        WHEN ep.age >= 80 AND ep.is_home_bp THEN '≥145/85'
        WHEN ep.age < 80 AND ep.is_clinic_bp THEN '≥140/90'
        WHEN ep.age < 80 AND ep.is_home_bp THEN '≥135/85'
    END AS applicable_threshold
FROM eligible_patients AS ep