{{ config(
    materialized='table',
    description='Aggregates neurology-related events for each person, providing analytics-ready person-level neurology event status and history.'
) }}

with person_level_neurology_aggregation as (
    select
        person_id,
        min(neurology_event_date) as earliest_neurology_event_date,
        max(neurology_event_date) as latest_neurology_event_date,
        array_agg(distinct neurology_observation_id) as all_neurology_observation_ids,
        array_agg(distinct neurology_concept_code) as all_neurology_concept_codes,
        array_agg(distinct neurology_concept_display) as all_neurology_concept_displays,
        array_agg(distinct neurology_code_category) as all_neurology_code_categories_applied
    from {{ ref('int_valproate_neurology_events') }}
    group by person_id
)

select
    pla.person_id,
    true as has_neurology_event,
    pla.earliest_neurology_event_date,
    pla.latest_neurology_event_date,
    pla.all_neurology_observation_ids,
    pla.all_neurology_concept_codes,
    pla.all_neurology_concept_displays,
    pla.all_neurology_code_categories_applied
from person_level_neurology_aggregation pla
-- Brief: Aggregates neurology events and status for valproate cohort, using intermediate neurology events table. Includes latest and historical neurology status, event details, and code traceability.
