{{ config(
    materialized='table',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: LTC LCS NHS Health Checks - Collects NHS health check completion observations for Long Term Conditions case finding exclusion logic.

Clinical Purpose:
• Gathers NHS health check completion data for case finding exclusion algorithms
• Supports identification of patients who have received recent health checks to avoid duplication
• Enables health check completion tracking for case finding programme logic
• Provides foundation data for excluding patients from case finding who have recent health assessments

Data Granularity:
• One row per NHS health check completion observation
• Covers health check completion events for case finding exclusion logic
• Sourced from LTC_LCS programme observation clusters for health check tracking
• Includes all historical and current NHS health check completion observations

Key Features:
• Cluster IDs: HEALTH_CHECK_COMP
• Supports health check completion tracking for case finding programme exclusions
• Health check lookback period logic for avoiding duplicate assessments
• Integration with LTC_LCS programme health check tracking systems'"
    ]
) }}
-- Intermediate model for NHS health check observations for LTC LCS case finding
-- Used for health check completion tracking and exclusion logic

{{ get_observations(
    cluster_ids="'HEALTH_CHECK_COMP'",
    source='LTC_LCS'
) }}
