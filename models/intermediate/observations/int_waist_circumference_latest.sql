{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Latest Waist Circumference Observations - Most recent valid waist circumference measurement per person.

Clinical Purpose:
• Provides current abdominal obesity status for cardiovascular risk assessment
• Supports metabolic syndrome screening and obesity management programmes
• Enables current anthropometric status evaluation and health risk stratification

Data Granularity:
• One row per person with their most recent valid waist circumference
• Includes only patients with valid measurements
• Filtered to exclude implausible values for clinical accuracy

Key Features:
• Latest valid waist circumference identification using comprehensive filtering
• Clinical risk categorisation for cardiovascular and metabolic assessment
• High-risk threshold flags for immediate clinical attention
• Complete measurement metadata for clinical context'"
        ]
    )
}}

/*
Latest valid waist circumference measurement per person.
Uses the comprehensive int_waist_circumference_all model and filters to most recent valid measurement.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    waist_circumference_value,
    concept_code,
    concept_display,
    source_cluster_id,
    waist_risk_category,
    is_high_waist_risk,
    is_very_high_waist_risk,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_waist_circumference_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_waist

WHERE is_valid_waist_circumference = TRUE
