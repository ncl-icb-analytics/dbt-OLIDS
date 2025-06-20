{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Latest valid serum creatinine measurement per person.
Uses the comprehensive int_creatinine_all model and filters to most recent valid creatinine.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    creatinine_value,
    concept_code,
    concept_display,
    source_cluster_id,
    creatinine_category,
    is_elevated_creatinine,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_creatinine_all'), 
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_creatinine

WHERE is_valid_creatinine = TRUE 