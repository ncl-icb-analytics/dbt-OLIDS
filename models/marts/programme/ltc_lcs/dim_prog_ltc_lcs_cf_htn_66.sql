{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'HTN_66 case finding: Stage 1 hypertension patients without cardiovascular risk factors (clinic ≥140/90, home ≥135/85)'"
) }}

-- HTN_66 case finding dimension: Stage 1 hypertension without cardiovascular risk factors
-- Identifies patients with stage 1 hypertension but no cardiovascular risk factors

SELECT
    person_id,
    age,
    has_cardiovascular_risk_factors,
    has_stage_1_hypertension_no_risk,
    latest_bp_date,
    latest_bp_value,
    latest_bp_type,
    is_clinic_bp,
    is_home_bp
FROM {{ ref('int_ltc_lcs_cf_htn_66') }} 