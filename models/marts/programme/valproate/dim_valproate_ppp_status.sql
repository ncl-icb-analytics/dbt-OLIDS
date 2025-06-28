{{ config(
    materialized='table',
    description='Aggregates Pregnancy Prevention Programme (PPP) events for each person, providing analytics-ready person-level PPP status and history.'
) }}

-- PPP events for each person from intermediate table
WITH base_ppp_observations AS (
    SELECT
        ppp.person_id,
        ppp.ppp_observation_id,
        ppp.ppp_event_date,
        ppp.ppp_concept_code,
        ppp.ppp_concept_display,
        ppp.ppp_categories,
        ppp.ppp_status_description,
        coalesce (ppp.ppp_status_description = 'Yes - PPP enrolled',
        FALSE) AS is_ppp_enrolled
    FROM {{ ref('int_ppp_status_all') }} AS ppp
),

latest_ppp_status AS (
    -- Most recent PPP event for each person
    SELECT
        person_id,
        ppp_observation_id,
        ppp_event_date,
        ppp_concept_code,
        ppp_concept_display,
        is_ppp_enrolled,
        ppp_status_description
    FROM base_ppp_observations
    QUALIFY
        row_number() OVER (PARTITION BY person_id ORDER BY ppp_event_date DESC)
        = 1
),

person_level_ppp_aggregation AS (
    SELECT
        person_id,
        min(ppp_event_date) AS earliest_ppp_event_date,
        max(ppp_event_date) AS latest_ppp_event_date,
        array_agg(DISTINCT ppp_observation_id) AS all_ppp_observation_ids,
        array_agg(DISTINCT ppp_concept_code) AS all_ppp_concept_codes,
        array_agg(DISTINCT ppp_concept_display) AS all_ppp_concept_displays,
        array_agg(DISTINCT ppp_categories[0]) AS all_ppp_code_categories_applied
    FROM base_ppp_observations
    GROUP BY person_id
)

SELECT
    pla.person_id,
    TRUE AS has_ppp_event,
    pla.earliest_ppp_event_date,
    pla.latest_ppp_event_date,
    latest.ppp_observation_id AS latest_ppp_observation_id,
    latest.ppp_concept_code AS latest_ppp_concept_code,
    latest.ppp_concept_display AS latest_ppp_concept_display,
    latest.is_ppp_enrolled AS is_currently_ppp_enrolled,
    latest.ppp_status_description AS current_ppp_status_description,
    pla.all_ppp_observation_ids,
    pla.all_ppp_concept_codes,
    pla.all_ppp_concept_displays,
    pla.all_ppp_code_categories_applied,
    NOT latest.is_ppp_enrolled AS is_ppp_non_enrolled,
    latest.ppp_status_description
    || ' ('
    || to_varchar(latest.ppp_event_date, 'DD/MM/YYYY')
    || ')' AS current_ppp_status_with_date
FROM person_level_ppp_aggregation AS pla
LEFT JOIN latest_ppp_status AS latest
    ON pla.person_id = latest.person_id

-- Brief: Aggregates PPP events and status for valproate cohort, using intermediate PPP events table and patient surrogate keys (not limited to active patients). Includes latest and historical PPP status, event details, and code traceability.
