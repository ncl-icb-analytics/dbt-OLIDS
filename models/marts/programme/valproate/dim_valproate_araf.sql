{{ config(
    materialized='table',
    description='Aggregates ARAF-related events for each person, providing analytics-ready person-level ARAF event status and history.') }}

WITH person_level_araf_aggregation AS (
    SELECT
        person_id,
        min(araf_event_date) AS earliest_araf_event_date,
        max(araf_event_date) AS latest_araf_event_date,
        max(
            CASE WHEN is_specific_araf_form_code THEN araf_event_date END
        ) AS latest_specific_araf_form_date,
        boolor_agg(is_specific_araf_form_code)
            AS has_specific_araf_form_meeting_lookback,
        array_agg(DISTINCT araf_observation_id) AS all_araf_observation_ids,
        array_agg(DISTINCT araf_concept_code) AS all_araf_concept_codes,
        array_agg(DISTINCT araf_concept_display) AS all_araf_concept_displays,
        array_agg(DISTINCT araf_code_category)
            AS all_araf_code_categories_applied
    FROM {{ ref('int_valproate_araf_events') }}
    GROUP BY person_id
)

SELECT
    pla.person_id,
    TRUE AS has_araf_event,
    pla.earliest_araf_event_date,
    pla.latest_araf_event_date,
    pla.latest_specific_araf_form_date,
    pla.all_araf_observation_ids,
    pla.all_araf_concept_codes,
    pla.all_araf_concept_displays,
    pla.all_araf_code_categories_applied,
    coalesce(pla.has_specific_araf_form_meeting_lookback, FALSE)
        AS has_specific_araf_form_meeting_lookback
FROM person_level_araf_aggregation AS pla
-- Brief: Aggregates ARAF events and status for valproate cohort, using intermediate ARAF events table. Includes latest and historical ARAF status, event details, and code traceability.
