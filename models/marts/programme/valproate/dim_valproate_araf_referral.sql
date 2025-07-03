{{ config(
    materialized='table',
    description='Aggregates ARAF referral-related events for each person, providing analytics-ready person-level ARAF referral event status and history.',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Mart: Valproate ARAF Referral Status - Person-level aggregation of ARAF referral events for valproate safety monitoring compliance.

Business Purpose:
• Support valproate safety monitoring through ARAF referral tracking and specialist engagement
• Enable systematic monitoring of specialist referral requirements for valproate therapy oversight
• Provide clinical decision support for ARAF referral compliance and specialist involvement
• Support quality improvement initiatives for comprehensive valproate safety programme delivery

Data Granularity:
• One row per person with ARAF referral events in valproate safety monitoring
• Aggregates all ARAF referral events with earliest and latest referral tracking
• Includes comprehensive referral history for specialist engagement monitoring

Key Features:
• Person-level ARAF referral event aggregation with complete specialist engagement history
• Earliest and latest referral tracking for monitoring specialist involvement effectiveness
• Evidence-based referral compliance assessment supporting comprehensive safety monitoring
• Integration with valproate safety pathways for systematic specialist engagement coordination'"
    ]
) }}

WITH person_level_araf_referral_aggregation AS (
    SELECT
        person_id,
        min(araf_referral_event_date) AS earliest_araf_referral_event_date,
        max(araf_referral_event_date) AS latest_araf_referral_event_date,
        array_agg(DISTINCT araf_referral_observation_id)
            AS all_araf_referral_observation_ids,
        array_agg(DISTINCT araf_referral_concept_code)
            AS all_araf_referral_concept_codes,
        array_agg(DISTINCT araf_referral_concept_display)
            AS all_araf_referral_concept_displays,
        array_agg(DISTINCT araf_referral_code_category)
            AS all_araf_referral_code_categories_applied
    FROM {{ ref('int_valproate_araf_referral_events') }}
    GROUP BY person_id
)

SELECT
    pla.person_id,
    TRUE AS has_araf_referral_event,
    pla.earliest_araf_referral_event_date,
    pla.latest_araf_referral_event_date,
    pla.all_araf_referral_observation_ids,
    pla.all_araf_referral_concept_codes,
    pla.all_araf_referral_concept_displays,
    pla.all_araf_referral_code_categories_applied
FROM person_level_araf_referral_aggregation AS pla
-- Brief: Aggregates ARAF referral events and status for valproate cohort, using intermediate ARAF referral events table. Includes latest and historical ARAF referral status, event details, and code traceability.
