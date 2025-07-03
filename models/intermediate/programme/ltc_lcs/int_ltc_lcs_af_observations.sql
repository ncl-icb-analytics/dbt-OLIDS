{{ config(
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: LTC LCS AF Observations - Collects all atrial fibrillation-relevant observations for Long Term Conditions case finding measures.

Clinical Purpose:
• Gathers comprehensive AF-related clinical observation data for case finding algorithms
• Supports identification of patients with undiagnosed atrial fibrillation through clinical observations
• Enables observation-based risk stratification for AF case finding measures
• Provides foundation data for AF_61 and AF_62 case finding indicators

Data Granularity:
• One row per clinical observation for AF-relevant observations
• Covers AF observations, exclusions, deep vein thrombosis, atrial flutter, pulse measurements
• Sourced from LTC_LCS programme observation clusters
• Includes all historical and current AF clinical observation patterns

Key Features:
• Cluster IDs: AF_OBSERVATIONS, AF_EXCLUSIONS, DEEP_VEIN_THROMBOSIS, ATRIAL_FLUTTER, ATRIAL_FIBRILLATION_61_EXCLUSIONS, PULSE_RATE, PULSE_RHYTHM
• Supports both AF_61 and AF_62 case finding measure requirements
• Comprehensive clinical observation analysis for undiagnosed AF detection
• Integration with LTC_LCS programme clinical observation tracking systems'"
    ]
) }}

-- Intermediate model for LTC LCS AF Observations
-- Collects all AF-relevant observations needed for all AF case finding measures

-- This intermediate fetches all AF-relevant observations for both AF_61 and AF_62 case finding measures
{{ get_observations(
    cluster_ids="'AF_OBSERVATIONS','AF_EXCLUSIONS','DEEP_VEIN_THROMBOSIS','ATRIAL_FLUTTER','ATRIAL_FIBRILLATION_61_EXCLUSIONS','PULSE_RATE','PULSE_RHYTHM'",
    source='LTC_LCS'
) }}
