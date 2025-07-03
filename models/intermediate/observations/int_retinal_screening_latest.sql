{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Latest Retinal Screening Observations - Most recent diabetes retinal screening completion per person.

Clinical Purpose:
• Provides current retinal screening status for diabetes care monitoring
• Supports QOF diabetes care process reporting and compliance assessment
• Enables current eye care status evaluation and screening programme management

Data Granularity:
• One row per person with their most recent completed retinal screening
• Includes only patients with completed screening records
• Focused on screening currency and compliance assessment

Key Features:
• Latest screening identification with currency flags (12m, 24m)
• Screening completion status for diabetes care pathway compliance
• Time-based screening status for QOF reporting
• Essential data for diabetes care process monitoring'"
        ]
    )
}}

/*
Latest diabetes retinal screening completion per person.
Used for diabetes care processes and screening programme analysis.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    is_completed_screening,
    days_since_screening,
    screening_current_12m,
    screening_current_24m

FROM (
    {{ get_latest_events(
        ref('int_retinal_screening_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_retinal_screening
