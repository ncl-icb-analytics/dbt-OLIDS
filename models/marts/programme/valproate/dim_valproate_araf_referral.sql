{{ config(
    materialized='table',
    description='Aggregates ARAF referral-related events for each person, providing analytics-ready person-level ARAF referral event status and history.'
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
