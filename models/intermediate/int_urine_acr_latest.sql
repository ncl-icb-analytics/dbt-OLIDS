{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Latest valid urine ACR measurement per person.
Uses the comprehensive int_urine_acr_all model and filters to most recent valid ACR.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    acr_value,
    concept_code,
    concept_display,
    source_cluster_id,
    acr_category,
    is_acr_elevated,
    is_microalbuminuria,
    is_macroalbuminuria,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_urine_acr_all'), 
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_acr

WHERE is_valid_acr = TRUE 