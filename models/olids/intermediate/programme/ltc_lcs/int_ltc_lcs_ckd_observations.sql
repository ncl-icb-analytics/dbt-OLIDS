-- Intermediate model for LTC LCS CKD Observations
-- Collects all CKD-relevant observations needed for CKD case finding measures

-- This intermediate fetches all CKD-relevant observations for case finding
-- Includes:
-- - EGFR_TESTING, EGFR_COD for CKD_61 (eGFR tests for consecutive low readings)
-- - UACR testing for CKD_62 and CKD_63
-- - Additional CKD-related observations for CKD_64 case finding measures
{{ get_observations(
    cluster_ids="'EGFR_TESTING', 'EGFR_COD', 'UACR_TESTING', 'CKD_ACUTE_KIDNEY_INJURY', 'CKD_BPH_GOUT', 'HAEMATURIA', 'LITHIUM_MEDICATIONS', 'SULFASALAZINE_MEDICATIONS', 'TACROLIMUS_MEDICATIONS', 'URINE_BLOOD_NEGATIVE', 'PROTEINURIA_FINDINGS'",
    source='LTC_LCS'
) }}
