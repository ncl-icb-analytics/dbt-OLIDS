{{ config(
    materialized='table') }}

-- Intermediate model for CVD-related medications for LTC LCS case finding
-- Includes statin medications, statin allergies/contraindications, and statin decisions

{{ get_medication_orders(
    cluster_id="'LCS_STAT_COD_CVD'",
    source='LTC_LCS'
) }}
