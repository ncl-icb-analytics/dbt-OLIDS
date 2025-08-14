{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All alcohol misuse disorder observations from clinical records.
Uses ALCOHOL_MISUSE_DISORDERS cluster containing codes that identify evidence of 
alcohol misuse, abuse, dependence, or harmful/hazardous alcohol use.
Includes both current and historical diagnostic codes.

Clinical Purpose:
- Alcohol misuse disorder tracking
- Substance use monitoring
- Population health assessment for alcohol-related conditions

This is OBSERVATION-LEVEL data - one row per alcohol disorder observation.
*/

WITH base_observations AS (
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value,
        obs.result_text,
        obs.is_problem,
        obs.problem_end_date

    FROM ({{ get_observations("'ALCOHOL_MISUSE_DISORDERS'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    result_value,
    result_text,
    is_problem,
    problem_end_date

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC