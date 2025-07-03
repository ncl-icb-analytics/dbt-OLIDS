{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Latest Urine ACR Observations - Most recent valid ACR measurement per person for kidney function assessment.

Clinical Purpose:
• Provides current kidney function status based on albumin-to-creatinine ratio
• Supports CKD staging and diabetic nephropathy assessment
• Enables current proteinuria status evaluation and clinical decision-making

Data Granularity:
• One row per person with their most recent valid ACR
• Includes only patients with valid ACR measurements
• Filtered to exclude implausible values for clinical accuracy

Key Features:
• Latest valid ACR identification using comprehensive filtering
• Clinical ACR categorisation for kidney damage assessment
• Microalbuminuria and macroalbuminuria flags for staging
• Complete observation metadata for clinical context'"
        ]
    )
}}

/*
Latest valid urine ACR measurement per person.
Uses the comprehensive int_urine_acr_all model and filters to most recent valid ACR.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    acr_value,
    concept_code,
    concept_display,
    source_cluster_id,
    acr_category,
    is_acr_elevated,
    is_microalbuminuria,
    is_macroalbuminuria,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_urine_acr_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_acr

WHERE is_valid_acr = TRUE
