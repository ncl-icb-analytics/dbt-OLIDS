{{ config(
    materialized='table',
    description='Aggregates Pregnancy Prevention Programme (PPP) events for each person, providing analytics-ready person-level PPP status and history.'
) }}

-- PPP events for each person from intermediate table
with base_ppp_observations as (
    select
        ppp.person_id,
        ppp.ppp_observation_id,
        ppp.ppp_event_date as ppp_event_date,
        ppp.ppp_concept_code as ppp_concept_code,
        ppp.ppp_concept_display as ppp_concept_display,
        ppp.ppp_categories as ppp_categories,
        case when ppp.ppp_status_description = 'Yes - PPP enrolled' then true else false end as is_ppp_enrolled,
        ppp.ppp_status_description
    from {{ ref('int_ppp_status_all') }} ppp
),

latest_ppp_status as (
    -- Most recent PPP event for each person
    select
        person_id,
        ppp_observation_id,
        ppp_event_date,
        ppp_concept_code,
        ppp_concept_display,
        is_ppp_enrolled,
        ppp_status_description
    from base_ppp_observations
    qualify row_number() over (partition by person_id order by ppp_event_date desc) = 1
),

person_level_ppp_aggregation as (
    select
        person_id,
        min(ppp_event_date) as earliest_ppp_event_date,
        max(ppp_event_date) as latest_ppp_event_date,
        array_agg(distinct ppp_observation_id) as all_ppp_observation_ids,
        array_agg(distinct ppp_concept_code) as all_ppp_concept_codes,
        array_agg(distinct ppp_concept_display) as all_ppp_concept_displays,
        array_agg(distinct ppp_categories[0]) as all_ppp_code_categories_applied
    from base_ppp_observations
    group by person_id
)

select
    pla.person_id,
    true as has_ppp_event,
    pla.earliest_ppp_event_date,
    pla.latest_ppp_event_date,
    latest.ppp_observation_id as latest_ppp_observation_id,
    latest.ppp_concept_code as latest_ppp_concept_code,
    latest.ppp_concept_display as latest_ppp_concept_display,
    latest.is_ppp_enrolled as is_currently_ppp_enrolled,
    not latest.is_ppp_enrolled as is_ppp_non_enrolled,
    latest.ppp_status_description as current_ppp_status_description,
    latest.ppp_status_description || ' (' || to_varchar(latest.ppp_event_date, 'DD/MM/YYYY') || ')' as current_ppp_status_with_date,
    pla.all_ppp_observation_ids,
    pla.all_ppp_concept_codes,
    pla.all_ppp_concept_displays,
    pla.all_ppp_code_categories_applied
from person_level_ppp_aggregation pla
left join latest_ppp_status latest
    on pla.person_id = latest.person_id

-- Brief: Aggregates PPP events and status for valproate cohort, using intermediate PPP events table and patient surrogate keys (not limited to active patients). Includes latest and historical PPP status, event details, and code traceability.
