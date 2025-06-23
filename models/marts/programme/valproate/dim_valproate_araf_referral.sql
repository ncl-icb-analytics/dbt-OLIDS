{{ config(
    materialized='table',
    description='Aggregates ARAF referral-related events for each person, providing analytics-ready person-level ARAF referral event status and history.'
) }}

with person_level_araf_referral_aggregation as (
    select
        person_id,
        min(araf_referral_event_date) as earliest_araf_referral_event_date,
        max(araf_referral_event_date) as latest_araf_referral_event_date,
        array_agg(distinct araf_referral_observation_id) as all_araf_referral_observation_ids,
        array_agg(distinct araf_referral_concept_code) as all_araf_referral_concept_codes,
        array_agg(distinct araf_referral_concept_display) as all_araf_referral_concept_displays,
        array_agg(distinct araf_referral_code_category) as all_araf_referral_code_categories_applied
    from {{ ref('int_valproate_araf_referral_events') }}
    group by person_id
)

select
    pla.person_id,
    true as has_araf_referral_event,
    pla.earliest_araf_referral_event_date,
    pla.latest_araf_referral_event_date,
    pla.all_araf_referral_observation_ids,
    pla.all_araf_referral_concept_codes,
    pla.all_araf_referral_concept_displays,
    pla.all_araf_referral_code_categories_applied
from person_level_araf_referral_aggregation pla
-- Brief: Aggregates ARAF referral events and status for valproate cohort, using intermediate ARAF referral events table. Includes latest and historical ARAF referral status, event details, and code traceability.
