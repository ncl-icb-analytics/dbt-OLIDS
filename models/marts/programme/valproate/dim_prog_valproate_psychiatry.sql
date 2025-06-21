{{ config(
    materialized='table',
    description='Aggregates psychiatry-related events for each person, providing analytics-ready person-level psychiatry event status and history.'
) }}

with person_level_psych_aggregation as (
    select
        person_id,
        min(psych_event_date) as earliest_psych_event_date,
        max(psych_event_date) as latest_psych_event_date,
        array_agg(distinct psych_observation_id) as all_psych_observation_ids,
        array_agg(distinct psych_concept_code) as all_psych_concept_codes,
        array_agg(distinct psych_concept_display) as all_psych_concept_displays,
        array_agg(distinct psych_code_category) as all_psych_code_categories_applied
    from {{ ref('int_valproate_psychiatry_events') }}
    group by person_id
)

select
    pla.person_id,
    true as has_psych_event,
    pla.earliest_psych_event_date,
    pla.latest_psych_event_date,
    pla.all_psych_observation_ids,
    pla.all_psych_concept_codes,
    pla.all_psych_concept_displays,
    pla.all_psych_code_categories_applied
from person_level_psych_aggregation pla
-- Brief: Aggregates psychiatry events and status for valproate cohort, using intermediate psychiatry events table. Includes latest and historical psychiatry status, event details, and code traceability.
