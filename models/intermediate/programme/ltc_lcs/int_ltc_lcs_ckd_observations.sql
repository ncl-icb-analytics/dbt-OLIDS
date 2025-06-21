-- Intermediate model for LTC LCS CKD Observations
-- Collects all CKD-relevant observations needed for CKD case finding measures

-- This intermediate fetches all CKD-relevant observations for case finding
-- Includes:
-- - URINE_ACR for CKD_61 and CKD_62 (urine ACR tests)
-- - EGFR_COD_LCS for CKD_61 (eGFR tests for consecutive low readings)
-- - Additional CKD-related observations for other case finding measures
{{ get_observations(
    cluster_ids="'UACR_TESTING', 'EGFR_COD_LCS', 'CKD_OBSERVATIONS', 'BP_READING', 'HBA1C', 'TOTAL_CHOLESTEROL', 'HDL_CHOLESTEROL', 'LDL_CHOLESTEROL', 'TRIGLYCERIDES', 'CKD_ACUTE_KIDNEY_INJURY', 'CKD_BPH_GOUT', 'LITHIUM_MEDICATIONS', 'SULFASALAZINE_MEDICATIONS', 'TACROLIMUS_MEDICATIONS', 'HAEMATURIA', 'URINE_BLOOD_NEGATIVE', 'PROTEINURIA_FINDINGS'",
    source='LTC_LCS'
) }}
