{{ config(
    materialized='table',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: LTC LCS DM Observations - Collects all diabetes-relevant observations for Long Term Conditions case finding measures.

Clinical Purpose:
• Gathers comprehensive diabetes-related clinical observation data for case finding algorithms
• Supports identification of patients with undiagnosed diabetes through laboratory and clinical observations
• Enables observation-based risk stratification using HbA1c measurements and diabetes risk scores
• Provides foundation data for diabetes case finding indicators including gestational diabetes risk

Data Granularity:
• One row per clinical observation for diabetes-relevant observations
• Covers HbA1c levels, QDiabetes risk scores, QRISK2 scores, BMI measurements
• Includes gestational diabetes history and pregnancy risk observations
• Sourced from LTC_LCS programme observation clusters

Key Features:
• Cluster IDs: HBA1C_LEVEL, QDIABETES_RISK, QRISK2_10YEAR, HISTORY_GESTATIONAL_DIABETES, GESTATIONAL_DIABETES_PREGNANCY_RISK, BMI_MEASUREMENT
• Supports diabetes case finding measures using laboratory and risk assessment data
• Comprehensive diabetes risk factor and diagnostic observation analysis
• Integration with LTC_LCS programme clinical observation tracking systems'"
    ]
) }}

-- Intermediate model for diabetes observations for LTC LCS case finding
-- Contains HbA1c measurements, diabetes risk scores, gestational diabetes history, and BMI measurements

{{ get_observations(
    cluster_ids="'HBA1C_LEVEL', 'QDIABETES_RISK', 'QRISK2_10YEAR', 'HISTORY_GESTATIONAL_DIABETES', 'GESTATIONAL_DIABETES_PREGNANCY_RISK', 'BMI_MEASUREMENT'",
    source='LTC_LCS'
) }}
