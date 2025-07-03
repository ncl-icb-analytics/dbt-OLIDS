{{ config(
    materialized='table',
    description='Aggregates neurology-related events for each person, providing analytics-ready person-level neurology event status and history.',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Mart: Valproate Neurology Status - Person-level aggregation of neurology-related events for valproate therapy monitoring and specialist care coordination.

Business Purpose:
• Support valproate safety monitoring through neurology specialist engagement tracking and care coordination
• Enable systematic monitoring of neurological care requirements for valproate therapy management
• Provide clinical decision support for neurology specialist involvement and treatment oversight
• Support quality improvement initiatives for comprehensive neurological care and medication safety

Data Granularity:
• One row per person with neurology-related events in valproate therapy monitoring
• Aggregates all neurology events with earliest and latest specialist engagement tracking
• Includes comprehensive neurology care history for specialist coordination assessment

Key Features:
• Person-level neurology event aggregation with complete specialist care history
• Earliest and latest neurology engagement tracking for monitoring care coordination effectiveness
• Evidence-based neurology care assessment supporting comprehensive valproate therapy management
• Integration with neurology care pathways for systematic specialist engagement and monitoring'"
    ]
) }}

WITH person_level_neurology_aggregation AS (
    SELECT
        person_id,
        min(neurology_event_date) AS earliest_neurology_event_date,
        max(neurology_event_date) AS latest_neurology_event_date,
        array_agg(DISTINCT neurology_observation_id)
            AS all_neurology_observation_ids,
        array_agg(DISTINCT neurology_concept_code)
            AS all_neurology_concept_codes,
        array_agg(DISTINCT neurology_concept_display)
            AS all_neurology_concept_displays,
        array_agg(DISTINCT neurology_code_category)
            AS all_neurology_code_categories_applied
    FROM {{ ref('int_valproate_neurology_events') }}
    GROUP BY person_id
)

SELECT
    pla.person_id,
    TRUE AS has_neurology_event,
    pla.earliest_neurology_event_date,
    pla.latest_neurology_event_date,
    pla.all_neurology_observation_ids,
    pla.all_neurology_concept_codes,
    pla.all_neurology_concept_displays,
    pla.all_neurology_code_categories_applied
FROM person_level_neurology_aggregation AS pla
-- Brief: Aggregates neurology events and status for valproate cohort, using intermediate neurology events table. Includes latest and historical neurology status, event details, and code traceability.
