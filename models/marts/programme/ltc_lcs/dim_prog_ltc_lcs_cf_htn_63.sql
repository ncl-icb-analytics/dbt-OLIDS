{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'HTN_63 case finding: Black/South Asian patients with cardiovascular risk factors and elevated BP (clinic ≥140/90, home ≥135/85)'"
) }}

-- HTN_63 case finding dimension: BSA patients with risk factors and elevated BP
-- Identifies BSA patients with cardiovascular risk factors and elevated blood pressure

SELECT
    person_id,
    age,
    is_bsa_with_risk_factors,
    has_elevated_bp_bsa,
    latest_bp_date,
    latest_bp_value,
    latest_bp_type,
    is_clinic_bp,
    is_home_bp
FROM {{ ref('int_ltc_lcs_cf_htn_63') }} 