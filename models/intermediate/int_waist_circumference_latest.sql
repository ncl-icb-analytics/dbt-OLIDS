{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Latest valid waist circumference measurement per person.
Uses the comprehensive int_waist_circumference_all model and filters to most recent valid measurement.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    waist_circumference_value,
    concept_code,
    concept_display,
    source_cluster_id,
    waist_risk_category,
    is_high_waist_risk,
    is_very_high_waist_risk,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_waist_circumference_all'), 
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_waist

WHERE is_valid_waist_circumference = TRUE 