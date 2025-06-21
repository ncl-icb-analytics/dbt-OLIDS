-- Mart model for LTC LCS Case Finding CVD_62
-- Identifies patients with QRISK2 score between 15-19.99% (case finding for cardiovascular disease prevention)

{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CVD_62 case finding dimension table for LTC/LCS programme. Identifies patients aged 40-83 with QRISK2 cardiovascular risk score between 15-19.99% who require cardiovascular disease prevention interventions. These are moderate-high risk patients who meet the criteria for lifestyle interventions and consideration for statin therapy. Used to prioritise patients for clinical review and cardiovascular risk assessment.'"
) }}

SELECT
    person_id,
    age,
    has_moderate_qrisk2,
    latest_qrisk2_date,
    latest_qrisk2_value,
    all_qrisk2_codes,
    all_qrisk2_displays,
    meets_criteria,
    CURRENT_TIMESTAMP() AS last_updated
FROM {{ ref('int_ltc_lcs_cf_cvd_62') }}
WHERE meets_criteria = TRUE  -- Only include patients who meet the criteria 