{{ config(
    description="CVD_66 case finding dimension table. Identifies patients aged 75-83 who need statin review due to lack of recent QRISK2 cardiovascular risk assessment. These are patients not currently on statins, with no statin allergies or contraindications, no recent statin clinical decisions, and no NHS health checks in the last 24 months. Used for LTC/LCS case finding to prioritise elderly patients for cardiovascular risk assessment and potential statin therapy.",
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CVD_66 case finding dimension table for LTC/LCS programme. Identifies patients aged 75-83 who need statin review due to lack of recent QRISK2 cardiovascular risk assessment. These are patients not currently on statins, with no statin allergies or contraindications, no recent statin clinical decisions, and no NHS health checks in the last 24 months. Used to prioritise elderly patients for cardiovascular risk assessment and potential statin therapy initiation.'"
) }}

-- CVD_66 case finding dimension: Statin review case finding
-- Identifies patients aged 75-83 who need statin review (no recent QRISK2 assessment)

SELECT
    person_id,
    age,
    needs_qrisk2_assessment,
    latest_qrisk2_date,
    latest_qrisk2_value,
    all_qrisk2_codes,
    all_qrisk2_displays
FROM {{ ref('int_ltc_lcs_cf_cvd_66') }} 