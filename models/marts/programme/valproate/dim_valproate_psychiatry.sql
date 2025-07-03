{{ config(
    materialized='table',
    description='Aggregates psychiatry-related events for each person, providing analytics-ready person-level psychiatry event status and history.',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Mart: Valproate Psychiatry Status - Person-level aggregation of psychiatry-related events for valproate therapy monitoring and mental health care coordination.

Business Purpose:
• Support valproate safety monitoring through psychiatry specialist engagement tracking and care coordination
• Enable systematic monitoring of mental health care requirements for valproate therapy management
• Provide clinical decision support for psychiatry specialist involvement and treatment oversight
• Support quality improvement initiatives for comprehensive mental health care and medication safety

Data Granularity:
• One row per person with psychiatry-related events in valproate therapy monitoring
• Aggregates all psychiatry events with earliest and latest specialist engagement tracking
• Includes comprehensive psychiatry care history for specialist coordination assessment

Key Features:
• Person-level psychiatry event aggregation with complete mental health specialist care history
• Earliest and latest psychiatry engagement tracking for monitoring care coordination effectiveness
• Evidence-based psychiatry care assessment supporting comprehensive valproate therapy management
• Integration with mental health care pathways for systematic specialist engagement and monitoring'"
    ]
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
