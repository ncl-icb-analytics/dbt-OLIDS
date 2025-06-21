{{ config(
    description="CVD_65 case finding dimension table. Identifies patients with QRISK2 cardiovascular risk score ≥ 10% who need moderate-dose statins for cardiovascular disease prevention. These are patients not currently on moderate-dose statins, with no statin allergies or contraindications, and no recent statin clinical decisions. Used for LTC/LCS case finding to prioritise patients for clinical review and statin therapy optimisation.",
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CVD_65 case finding dimension table for LTC/LCS programme. Identifies patients with QRISK2 cardiovascular risk score ≥ 10% who need moderate-dose statins for cardiovascular disease prevention. These are patients not currently on moderate-dose statins, with no statin allergies or contraindications, and no recent statin clinical decisions. Used to prioritise patients for clinical review and statin therapy optimisation based on their elevated cardiovascular risk.'"
) }}

-- CVD_65 case finding dimension: Moderate-dose statin case finding
-- Identifies patients with QRISK2 ≥ 10 who need moderate-dose statins

SELECT
    person_id,
    age,
    needs_moderate_dose_statin,
    latest_qrisk2_date,
    latest_qrisk2_value,
    all_qrisk2_codes,
    all_qrisk2_displays
FROM {{ ref('int_ltc_lcs_cf_cvd_65') }} 