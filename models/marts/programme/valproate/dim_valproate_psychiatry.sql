{{ config(
    materialized='table',
    description='Aggregates psychiatry-related events for each person, providing analytics-ready person-level psychiatry event status and history.'
) }}

WITH person_level_psych_aggregation AS (
    SELECT
        person_id,
        min(psych_event_date) AS earliest_psych_event_date,
        max(psych_event_date) AS latest_psych_event_date,
        array_agg(DISTINCT psych_observation_id) AS all_psych_observation_ids,
        array_agg(DISTINCT psych_concept_code) AS all_psych_concept_codes,
        array_agg(DISTINCT psych_concept_display) AS all_psych_concept_displays,
        array_agg(DISTINCT psych_code_category)
            AS all_psych_code_categories_applied
    FROM {{ ref('int_valproate_psychiatry_events') }}
    GROUP BY person_id
)

SELECT
    pla.person_id,
    TRUE AS has_psych_event,
    pla.earliest_psych_event_date,
    pla.latest_psych_event_date,
    pla.all_psych_observation_ids,
    pla.all_psych_concept_codes,
    pla.all_psych_concept_displays,
    pla.all_psych_code_categories_applied
FROM person_level_psych_aggregation AS pla
-- Brief: Aggregates psychiatry events and status for valproate cohort, using intermediate psychiatry events table. Includes latest and historical psychiatry status, event details, and code traceability.
