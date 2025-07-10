{{ config(
    materialized='table') }}

-- CYP Asthma medications for LTC/LCS case finding
-- Includes asthma medications, prednisolone, and montelukast

{{ get_medication_orders(
    cluster_id="'ASTHMA_MEDICATIONS','ASTHMA_PREDNISOLONE','MONTELUKAST_MEDICATIONS'",
    source='LTC_LCS'
) }}
