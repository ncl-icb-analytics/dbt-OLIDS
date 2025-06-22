{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'HTN_61 case finding: Severe hypertension patients (clinic BP ≥180/120, home BP ≥170/115)'"
) }}

-- HTN_61 case finding dimension: Severe hypertension
-- Identifies patients with severe hypertension requiring immediate intervention

SELECT
    person_id,
    age,
    has_severe_hypertension,
    latest_bp_date,
    latest_bp_value,
    latest_bp_type,
    is_clinic_bp,
    is_home_bp
FROM {{ ref('int_ltc_lcs_cf_htn_61') }} 