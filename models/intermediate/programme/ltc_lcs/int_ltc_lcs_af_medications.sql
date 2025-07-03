{{ config(
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: LTC LCS AF Medications - Collects all atrial fibrillation-relevant medications for Long Term Conditions case finding measures.

Clinical Purpose:
• Gathers comprehensive AF-related medication data for case finding algorithms
• Supports identification of patients with undiagnosed atrial fibrillation through medication patterns
• Enables medication-based risk stratification for AF case finding measures
• Provides foundation data for AF_61 and AF_62 case finding indicators

Data Granularity:
• One row per medication order for AF-relevant medications
• Covers oral anticoagulants, AF medications, digoxin, and cardiac glycosides
• Sourced from LTC_LCS programme medication clusters
• Includes all historical and current AF medication patterns

Key Features:
• Cluster IDs: ORAL_ANTICOAGULANT_2_8_2, AF_MEDICATIONS, DIGOXIN_MEDICATIONS, CARDIAC_GLYCOSIDES
• Supports both AF_61 and AF_62 case finding measure requirements
• Comprehensive medication pattern analysis for undiagnosed AF detection
• Integration with LTC_LCS programme medication tracking systems'"
    ]
) }}

-- Intermediate model for LTC LCS AF Medications
-- Collects all AF-relevant medications needed for all AF case finding measures

-- Use the macro to fetch all medication orders for AF clusters
-- (Add or adjust cluster_ids as needed for all AF-relevant meds)

{{ get_medication_orders(
    cluster_id="'ORAL_ANTICOAGULANT_2_8_2','AF_MEDICATIONS','DIGOXIN_MEDICATIONS','CARDIAC_GLYCOSIDES'",
    source="LTC_LCS"
) }}
