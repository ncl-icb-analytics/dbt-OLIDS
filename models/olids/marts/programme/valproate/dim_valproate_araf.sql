{{ config(
    materialized='table',
    description='Aggregates ARAF-related events for each person, providing analytics-ready person-level ARAF event status and history.') }}

WITH person_level_araf_aggregation AS (
    SELECT
        person_id,
        min(araf_event_date) AS earliest_araf_event_date,
        max(araf_event_date) AS latest_araf_event_date,
        max(
            CASE WHEN is_specific_araf_form_code THEN araf_event_date END
        ) AS latest_specific_araf_form_date,
        max(
            CASE WHEN araf_concept_code = '1366401000000107' THEN araf_event_date END
        ) AS latest_old_araf_date,
        max(
            CASE WHEN araf_concept_code = '2078951000000106' THEN araf_event_date END
        ) AS latest_new_araf_date,
        boolor_agg(is_specific_araf_form_code)
            AS has_specific_araf_form_meeting_lookback,
        array_agg(DISTINCT araf_observation_id) AS all_araf_observation_ids,
        array_agg(DISTINCT araf_concept_code) AS all_araf_concept_codes,
        array_agg(DISTINCT araf_concept_display) AS all_araf_concept_displays,
        array_agg(DISTINCT araf_code_category)
            AS all_araf_code_categories_applied
    FROM {{ ref('int_valproate_araf_events') }}
    GROUP BY person_id
)

SELECT
    pla.person_id,
    TRUE AS has_araf_event,
    pla.earliest_araf_event_date,
    pla.latest_araf_event_date,
    pla.latest_specific_araf_form_date,
    pla.latest_old_araf_date,
    pla.latest_new_araf_date,
    pla.all_araf_observation_ids,
    pla.all_araf_concept_codes,
    pla.all_araf_concept_displays,
    pla.all_araf_code_categories_applied,
    coalesce(pla.has_specific_araf_form_meeting_lookback, FALSE)
        AS has_specific_araf_form_meeting_lookback,
    
    -- ARAF form type flags
    array_contains('1366401000000107'::VARIANT, pla.all_araf_concept_codes) AS has_old_annual_risk_acknowledgement_form,
    array_contains('2078951000000106'::VARIANT, pla.all_araf_concept_codes) AS has_new_annual_risk_acknowledgement_form,
    
    -- Latest ARAF overall date (matching legacy logic: prioritise new over old if both exist)
    CASE 
        WHEN pla.latest_new_araf_date IS NOT NULL THEN 
            CASE 
                WHEN pla.latest_old_araf_date IS NOT NULL AND pla.latest_old_araf_date > pla.latest_new_araf_date 
                THEN pla.latest_old_araf_date 
                ELSE pla.latest_new_araf_date 
            END
        ELSE pla.latest_old_araf_date 
    END AS latest_araf_overall_date,
    
    -- ARAF form type info with actual dates for each form type
    CASE 
        WHEN pla.latest_old_araf_date IS NOT NULL 
        THEN 'Yes (' || TO_VARCHAR(pla.latest_old_araf_date, 'DD/MM/YYYY') || ')' 
        ELSE 'No' 
    END AS old_annual_risk_acknowledgement_form_info,
    
    CASE 
        WHEN pla.latest_new_araf_date IS NOT NULL 
        THEN 'Yes (' || TO_VARCHAR(pla.latest_new_araf_date, 'DD/MM/YYYY') || ')' 
        ELSE 'No' 
    END AS new_annual_risk_acknowledgement_form_info
    
FROM person_level_araf_aggregation AS pla
-- Brief: Aggregates ARAF events and status for valproate cohort, using intermediate ARAF events table. Includes latest and historical ARAF status, event details, and code traceability.
