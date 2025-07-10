{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid BMI measurement per person.
Uses the comprehensive int_bmi_all model and filters to most recent valid BMI.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    bmi_value,
    concept_code,
    concept_display,
    source_cluster_id,
    bmi_category,
    original_result_value,
    is_valid_bmi

FROM (
    {{ get_latest_events(
        ref('int_bmi_all'),
        partition_by=['person_id'],
        order_by=['clinical_effective_date']
    ) }}
)
WHERE is_valid_bmi = TRUE
