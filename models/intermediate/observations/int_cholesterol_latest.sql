{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Latest Cholesterol Observations - Most recent valid cholesterol measurement per person.

Clinical Purpose:
• Provides current cholesterol status for cardiovascular risk assessment
• Supports lipid management and statin therapy decision-making
• Enables current cardiovascular risk stratification and QOF reporting

Data Granularity:
• One row per person with their most recent valid cholesterol
• Includes only patients with valid cholesterol measurements
• Filtered to exclude implausible values for clinical accuracy

Key Features:
• Latest valid cholesterol identification using comprehensive filtering
• Clinical cholesterol categorisation for risk assessment
• Data quality assurance with validation flags
• Complete observation metadata for clinical context'"
        ]
    )
}}

/*
Latest valid total cholesterol measurement per person.
Uses the comprehensive int_cholesterol_all model and filters to most recent valid cholesterol.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    cholesterol_value,
    concept_code,
    concept_display,
    source_cluster_id,
    cholesterol_category,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_cholesterol_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_cholesterol

WHERE is_valid_cholesterol = TRUE
