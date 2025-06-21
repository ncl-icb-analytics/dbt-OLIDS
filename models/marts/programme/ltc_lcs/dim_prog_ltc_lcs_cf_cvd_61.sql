-- Mart model for LTC LCS Case Finding CVD_61
-- Identifies patients with QRISK2 score ≥ 20% (case finding for cardiovascular disease prevention)

{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CVD_61 case finding dimension table for LTC/LCS programme. Identifies patients aged 40-83 with QRISK2 cardiovascular risk score ≥ 20% who require urgent cardiovascular disease prevention interventions. These are high-risk patients who meet the criteria for primary prevention with statins and lifestyle interventions. Used to prioritise patients for immediate clinical review and cardiovascular risk reduction strategies.'"
) }}

SELECT
    person_id,
    age,
    has_high_qrisk2,
    latest_qrisk2_date,
    latest_qrisk2_value,
    all_qrisk2_codes,
    all_qrisk2_displays,
    meets_criteria,
    CURRENT_TIMESTAMP() AS last_updated
FROM {{ ref('int_ltc_lcs_cf_cvd_61') }}
WHERE meets_criteria = TRUE  -- Only include patients who meet the criteria 