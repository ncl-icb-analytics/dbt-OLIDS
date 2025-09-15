-- Intermediate model for LTC LCS CKD Observations
-- Collects all CKD-relevant observations needed for CKD case finding measures

-- This intermediate fetches all CKD-relevant observations for case finding
-- Includes:
-- - EGFR_COD_LCS, EGFR_COD for CKD_61 (eGFR tests for consecutive low readings)
-- - UACR testing for CKD_62 and CKD_63
-- - Additional CKD-related observations for CKD_64 case finding measures
{{ get_observations(
    cluster_ids="'EGFR_COD_LCS', 'EGFR_COD', 'UACR_TESTING', 'CKD_AKI', 'CKD_BPH_GOUT', 'Hematuria', 'LITHIUM_MEDICATIONS', 'SULFASALAZINE_MEDICATIONS', 'TACROLIMUS_MEDICATIONS', 'Urine_Blood_Neg', 'Proteinuria'",
    source='LTC_LCS'
) }}
