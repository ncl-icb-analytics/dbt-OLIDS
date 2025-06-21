{{ config(
    materialized='table',
    description='Aggregates ARAF-related events for each person, providing analytics-ready person-level ARAF event status and history.'
) }}

with person_level_araf_aggregation as (
    select
        person_id,
        min(araf_event_date) as earliest_araf_event_date,
        max(araf_event_date) as latest_araf_event_date,
        max(case when is_specific_araf_form_code then araf_event_date else null end) as latest_specific_araf_form_date,
        boolor_agg(is_specific_araf_form_code) as has_specific_araf_form_meeting_lookback,
        array_agg(distinct araf_observation_id) as all_araf_observation_ids,
        array_agg(distinct araf_concept_code) as all_araf_concept_codes,
        array_agg(distinct araf_concept_display) as all_araf_concept_displays,
        array_agg(distinct araf_code_category) as all_araf_code_categories_applied
    from {{ ref('int_valproate_araf_events') }}
    group by person_id
)

select
    pla.person_id,
    true as has_araf_event,
    pla.earliest_araf_event_date,
    pla.latest_araf_event_date,
    pla.latest_specific_araf_form_date,
    coalesce(pla.has_specific_araf_form_meeting_lookback, false) as has_specific_araf_form_meeting_lookback,
    pla.all_araf_observation_ids,
    pla.all_araf_concept_codes,
    pla.all_araf_concept_displays,
    pla.all_araf_code_categories_applied
from person_level_araf_aggregation pla
-- Brief: Aggregates ARAF events and status for valproate cohort, using intermediate ARAF events table. Includes latest and historical ARAF status, event details, and code traceability.
