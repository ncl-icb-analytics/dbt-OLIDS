{{ config(
    materialized='table') }}

-- Intermediate model for CVD-related observations for LTC LCS case finding
-- Includes QRISK2 scores, cholesterol measurements, statin allergies/adverse reactions,
-- statin contraindications, and statin clinical decisions

{{ get_observations(
    cluster_ids="'QRISK2_10YEAR', 'NON_HDL_CHOLESTEROL', 'STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED', 'STATINDEC_COD'",
    source='LTC_LCS'
) }}
