{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'HTN_62 case finding: Stage 2 hypertension patients (clinic BP ≥160/100, home BP ≥150/95), excluding severe hypertension (HTN_61) patients'"
) }}

-- HTN_62 case finding dimension: Stage 2 hypertension (excluding HTN_61)
-- Identifies patients with stage 2 hypertension who are not already in HTN_61

SELECT
    person_id,
    age,
    has_stage_2_hypertension,
    latest_bp_date,
    latest_bp_value,
    latest_bp_type,
    is_clinic_bp,
    is_home_bp
FROM {{ ref('int_ltc_lcs_cf_htn_62') }} 