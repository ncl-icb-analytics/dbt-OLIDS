{{ config(
    description="CVD_64 case finding dimension table. Identifies patients aged 40-83 who need high-dose statins for cardiovascular disease prevention. These are patients not currently on statins, with no statin allergies or contraindications, and no recent statin clinical decisions. Used for LTC/LCS case finding to prioritise patients for clinical review and statin initiation.",
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CVD_64 case finding dimension table for LTC/LCS programme. Identifies patients aged 40-83 who need high-dose statins for cardiovascular disease prevention. These are patients not currently on statins, with no statin allergies or contraindications, and no recent statin clinical decisions. Used to prioritise patients for clinical review and statin initiation to reduce cardiovascular risk.'"
) }}

-- CVD_64 case finding dimension: High-dose statin case finding
-- Identifies patients who need high-dose statins

SELECT
    person_id,
    age,
    needs_high_dose_statin,
    latest_statin_date,
    latest_statin_code,
    latest_statin_display,
    all_statin_codes,
    all_statin_displays
FROM {{ ref('int_ltc_lcs_cf_cvd_64') }} 