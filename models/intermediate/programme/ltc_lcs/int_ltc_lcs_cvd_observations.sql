{{ config(
    materialized='table',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: LTC LCS CVD Observations - Collects all cardiovascular disease-relevant observations for Long Term Conditions case finding measures.

Clinical Purpose:
• Gathers comprehensive CVD-related clinical observation data for case finding algorithms
• Supports identification of patients requiring CVD risk assessment through clinical observations
• Enables observation-based risk stratification using QRISK2 scores and cholesterol measurements
• Provides foundation data for CVD case finding indicators including statin decision tracking

Data Granularity:
• One row per clinical observation for CVD-relevant observations
• Covers QRISK2 10-year scores, non-HDL cholesterol measurements
• Includes statin allergy/adverse reactions, contraindications, and clinical decisions
• Sourced from LTC_LCS programme observation clusters

Key Features:
• Cluster IDs: QRISK2_10YEAR, NON_HDL_CHOLESTEROL, STATIN_ALLERGY_ADVERSE_REACTION, STATIN_NOT_INDICATED, STATINDEC_COD
• Supports cardiovascular risk assessment and primary prevention case finding
• Comprehensive clinical decision tracking for statin prescribing
• Integration with LTC_LCS programme clinical observation tracking systems'"
    ]
) }}

-- Intermediate model for CVD-related observations for LTC LCS case finding
-- Includes QRISK2 scores, cholesterol measurements, statin allergies/adverse reactions,
-- statin contraindications, and statin clinical decisions

{{ get_observations(
    cluster_ids="'QRISK2_10YEAR', 'NON_HDL_CHOLESTEROL', 'STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED', 'STATINDEC_COD'",
    source='LTC_LCS'
) }}
