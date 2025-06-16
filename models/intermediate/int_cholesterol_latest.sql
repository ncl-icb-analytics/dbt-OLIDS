{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Latest valid total cholesterol measurement per person.
Uses the comprehensive int_cholesterol_all model and filters to most recent valid cholesterol.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    cholesterol_value,
    concept_code,
    concept_display,
    source_cluster_id,
    cholesterol_category,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_cholesterol_all'), 
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_cholesterol

WHERE is_valid_cholesterol = TRUE 