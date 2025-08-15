{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All NHS Health Check completed events for health check programme monitoring.
Uses HEALTH_CHECK_COMP cluster for completed health checks.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,

    -- All records represent completed NHS Health Checks
    TRUE AS is_completed_health_check,

    -- Health check currency flags (standard intervals)
    CASE
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS health_check_current_12m,

    CASE
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 730 THEN TRUE
        ELSE FALSE
    END AS health_check_current_24m,

    -- NHS Health Check cycle (5 years)
    CASE
        WHEN DATEDIFF(day, obs.clinical_effective_date, CURRENT_DATE()) <= 1825 THEN TRUE
        ELSE FALSE
    END AS health_check_current_5y

FROM ({{ get_observations("'HEALTH_CHECK_COMP'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
ORDER BY person_id, clinical_effective_date DESC
