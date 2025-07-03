{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Latest eGFR Observations - Most recent valid eGFR measurement per person with CKD staging.

Clinical Purpose:
• Provides current kidney function status based on estimated glomerular filtration rate
• Supports CKD monitoring and clinical decision-making
• Enables current CKD stage assessment and progression tracking

Data Granularity:
• One row per person with their most recent valid eGFR
• Includes only patients with valid eGFR measurements
• Filtered to exclude implausible values for clinical accuracy

Key Features:
• Latest valid eGFR identification using comprehensive filtering
• CKD stage classification (stages 1-5) for clinical management
• CKD indicator flags for immediate clinical attention
• Complete observation metadata for clinical context'"
        ]
    )
}}

/*
Latest valid eGFR measurement per person.
Uses the comprehensive int_egfr_all model and filters to most recent valid eGFR.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    egfr_value,
    concept_code,
    concept_display,
    source_cluster_id,
    ckd_stage,
    is_ckd_indicator,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_egfr_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_egfr

WHERE is_valid_egfr = TRUE
