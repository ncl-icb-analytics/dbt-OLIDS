{{ config(
    materialized='table',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: LTC LCS CYP Asthma Observations - Collects all children and young people asthma-relevant observations for Long Term Conditions case finding measures.

Clinical Purpose:
• Gathers comprehensive CYP asthma-related clinical observation data for case finding algorithms
• Supports identification of children and young people with undiagnosed asthma through clinical observations
• Enables observation-based risk stratification for paediatric asthma case finding measures
• Provides foundation data for CYP asthma case finding indicators including suspected asthma and wheeze

Data Granularity:
• One row per clinical observation for CYP asthma-relevant observations
• Covers suspected asthma, viral wheeze, asthma diagnosis, and asthma resolved observations
• Sourced from LTC_LCS programme observation clusters for paediatric populations
• Includes all historical and current asthma clinical observation patterns for children and young people

Key Features:
• Cluster IDs: SUSPECTED_ASTHMA, VIRAL_WHEEZE, ASTHMA_DIAGNOSIS, ASTHMA_RESOLVED
• Supports CYP-specific asthma case finding measure requirements
• Comprehensive paediatric clinical observation analysis for undiagnosed asthma detection
• Integration with LTC_LCS programme clinical observation tracking systems for children and young people'"
    ]
) }}

-- CYP Asthma observations for LTC/LCS case finding
-- Includes asthma diagnoses, symptoms, and related conditions (non-medication observations)

{{ get_observations(
    cluster_ids="'SUSPECTED_ASTHMA','VIRAL_WHEEZE','ASTHMA_DIAGNOSIS','ASTHMA_RESOLVED'",
    source='LTC_LCS'
) }}
