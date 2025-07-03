{{ config(
    materialized='table',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: LTC LCS CYP Asthma Medications - Collects all children and young people asthma-relevant medications for Long Term Conditions case finding measures.

Clinical Purpose:
• Gathers comprehensive CYP asthma-related medication data for case finding algorithms
• Supports identification of children and young people with undiagnosed asthma through medication patterns
• Enables medication-based risk stratification for paediatric asthma case finding measures
• Provides foundation data for CYP asthma case finding indicators

Data Granularity:
• One row per medication order for CYP asthma-relevant medications
• Covers asthma medications, prednisolone (oral steroids), and montelukast (leukotriene receptor antagonist)
• Sourced from LTC_LCS programme medication clusters for paediatric populations
• Includes all historical and current asthma medication patterns for children and young people

Key Features:
• Cluster IDs: ASTHMA_MEDICATIONS, ASTHMA_PREDNISOLONE, MONTELUKAST_MEDICATIONS
• Supports CYP-specific asthma case finding measure requirements
• Comprehensive paediatric medication pattern analysis for undiagnosed asthma detection
• Integration with LTC_LCS programme medication tracking systems for children and young people'"
    ]
) }}

-- CYP Asthma medications for LTC/LCS case finding
-- Includes asthma medications, prednisolone, and montelukast

{{ get_medication_orders(
    cluster_id="'ASTHMA_MEDICATIONS','ASTHMA_PREDNISOLONE','MONTELUKAST_MEDICATIONS'",
    source='LTC_LCS'
) }}
