{{ config(
    materialized='table') }}

-- Intermediate model for diabetes observations for LTC LCS case finding
-- Contains HbA1c measurements, diabetes risk scores, gestational diabetes history, and BMI measurements

{{ get_observations(
    cluster_ids="'HBA1C_LEVEL', 'HBA1C', 'QDIABETES_RISK', 'QRISK2_10YEAR', 'GESTDIAB_COD', 'DM_GESTDIAB_AND_PREG_RISK', 'BMI_CODES', 'BAME_ETHNICITY', 'WHITE_BRITISH', 'DM_EXCL_ETHNICITY', 'NDH_COD', 'PRD_COD'",
    source='LTC_LCS'
) }}
