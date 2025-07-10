{{ config(
    materialized='table') }}

-- HTN_63 case finding: Black/South Asian patients with cardiovascular risk factors and elevated BP
-- Identifies BSA patients with risk factors and elevated blood pressure

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
    INNER JOIN {{ ref('int_ltc_lcs_cf_base_population') }} AS base ON bp.person_id = base.person_id
    QUALIFY
        row_number()
            OVER (
                PARTITION BY bp.person_id
                ORDER BY bp.clinical_effective_date DESC
            )
        = 1
),

bsa_patients AS (
    -- BSA patients only
    SELECT DISTINCT person_id
    FROM {{ ref('int_ltc_lcs_ethnicity_observations') }}
    WHERE cluster_id = 'ETHNICITY_BAME'
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
    )
),

bsa_with_risk_factors AS (
    -- BSA patients with cardiovascular risk factors
    SELECT
        base.person_id,
        base.age,
        TRUE AS has_cardiovascular_risk_factors
    FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS base
    INNER JOIN bsa_patients ON base.person_id = bsa_patients.person_id
    INNER JOIN risk_factor_patients ON base.person_id = risk_factor_patients.person_id
),

eligible_patients AS (
-- BSA patients with risk factors and elevated BP
    SELECT
        bp.person_id,
        bsa.age,
        bp.latest_bp_date,
        bp.systolic_value AS latest_bp_value,
        bp.latest_bp_type,
        bp.is_clinic_bp,
        bp.is_home_bp,
        TRUE AS is_bsa_with_risk_factors,
        TRUE AS has_elevated_bp_bsa
    FROM latest_bp AS bp
    INNER JOIN bsa_with_risk_factors AS bsa ON bp.person_id = bsa.person_id
    WHERE (
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

-- Final selection: BSA patients with risk factors and elevated BP
SELECT
    ep.person_id,
    ep.age,
    ep.is_bsa_with_risk_factors,
    ep.has_elevated_bp_bsa,
    ep.latest_bp_date,
    ep.latest_bp_value,
    ep.latest_bp_type,
    ep.is_clinic_bp,
    ep.is_home_bp
FROM eligible_patients AS ep
