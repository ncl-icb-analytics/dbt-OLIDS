{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: NHS Health Check Latest - Latest NHS Health Check completion per person for programme analysis and eligibility assessment.

Clinical Purpose:
• Provides most recent NHS Health Check completion status for each person
• Supports health check programme analysis and eligibility assessment for current status
• Enables identification of patients requiring health check invitations or follow-up
• Provides foundation data for health check currency tracking and programme planning

Data Granularity:
• One row per person representing their most recent NHS Health Check completion
• Derived from comprehensive NHS Health Check events using latest event logic
• Includes key temporal flags for health check currency (12m, 24m, 5y intervals)
• Contains essential health check completion indicators and time calculations

Key Features:
• Latest event selection per person using clinical_effective_date ordering
• Health check currency flags for programme eligibility assessment
• Time-based calculations for programme planning and invitation scheduling
• Integration with comprehensive NHS Health Check events for complete programme analysis'"
        ]
    )
}}

/*
Latest NHS Health Check completion per person.
Used for health check programme analysis and eligibility assessment.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    is_completed_health_check,
    days_since_health_check,
    health_check_current_12m,
    health_check_current_24m,
    health_check_current_5y,
    years_since_health_check

FROM (
    {{ get_latest_events(
        ref('int_nhs_health_check_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_health_check
