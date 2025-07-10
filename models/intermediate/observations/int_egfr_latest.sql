{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid eGFR measurement per person.
Uses the comprehensive int_egfr_all model and filters to most recent valid eGFR.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    egfr_value,
    concept_code,
    concept_display,
    source_cluster_id,
    ckd_stage,
    is_ckd_indicator,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_egfr_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_egfr

WHERE is_valid_egfr = TRUE
