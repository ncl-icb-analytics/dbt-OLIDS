{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Latest Creatinine Observations - Most recent valid creatinine measurement per person.

Clinical Purpose:
• Provides current kidney function status based on serum creatinine levels
• Supports renal disease assessment and medication safety monitoring
• Enables current CKD risk stratification and clinical decision-making

Data Granularity:
• One row per person with their most recent valid creatinine
• Includes only patients with valid creatinine measurements
• Filtered to exclude implausible values for clinical accuracy

Key Features:
• Latest valid creatinine identification using comprehensive filtering
• Clinical creatinine categorisation for kidney function assessment
• Elevated creatinine flags for immediate clinical attention
• Complete observation metadata for clinical context'"
        ]
    )
}}

/*
Latest valid serum creatinine measurement per person.
Uses the comprehensive int_creatinine_all model and filters to most recent valid creatinine.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    creatinine_value,
    concept_code,
    concept_display,
    source_cluster_id,
    creatinine_category,
    is_elevated_creatinine,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_creatinine_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_creatinine

WHERE is_valid_creatinine = TRUE
