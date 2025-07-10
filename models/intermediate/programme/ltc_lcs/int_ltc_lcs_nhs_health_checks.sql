{{ config(
    materialized='table') }}
-- Intermediate model for NHS health check observations for LTC LCS case finding
-- Used for health check completion tracking and exclusion logic

{{ get_observations(
    cluster_ids="'HEALTH_CHECK_COMP'",
    source='LTC_LCS'
) }}
