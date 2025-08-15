{{ config(
    materialized='table') }}

-- Intermediate model for diabetes observations for LTC LCS case finding
-- Contains HbA1c measurements, diabetes risk scores, gestational diabetes history, and BMI measurements

{{ get_observations(
    cluster_ids="'HBA1C_LEVEL', 'QDIABETES_RISK', 'QRISK2_10YEAR', 'HISTORY_GESTATIONAL_DIABETES', 'GESTATIONAL_DIABETES_PREGNANCY_RISK', 'BMI_MEASUREMENT'",
    source='LTC_LCS'
) }}
