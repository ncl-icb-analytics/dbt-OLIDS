{{ config(
    materialized='table') }}

-- CYP Asthma observations for LTC/LCS case finding
-- Includes asthma diagnoses, symptoms, and related conditions (non-medication observations)

{{ get_observations(
    cluster_ids="'SUSPECTED_ASTHMA','VIRAL_WHEEZE','ASTHMA_DIAGNOSIS','ASTHMA_RESOLVED'",
    source='LTC_LCS'
) }}
