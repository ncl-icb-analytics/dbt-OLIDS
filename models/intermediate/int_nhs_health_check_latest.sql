{{
    config(
        materialized='table',
        cluster_by=['person_id']
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

FROM {{ ref('int_nhs_health_check_all') }}
{{ get_latest_events(partition_by=['person_id'], order_by='clinical_effective_date') }} 