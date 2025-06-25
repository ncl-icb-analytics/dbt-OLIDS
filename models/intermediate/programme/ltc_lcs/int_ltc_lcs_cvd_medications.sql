{{ config(materialized='table') }}

-- Intermediate model for CVD-related medications for LTC LCS case finding
-- Includes statin medications, statin allergies/contraindications, and statin decisions

{{ get_medication_orders(
    cluster_id=['STATIN_CVD_MEDICATIONS', 'STATIN_CVD_63_MEDICATIONS', 'STATIN_CVD_64_MEDICATIONS', 'STATIN_CVD_65_MEDICATIONS'],
    source='LTC_LCS'
) }}
