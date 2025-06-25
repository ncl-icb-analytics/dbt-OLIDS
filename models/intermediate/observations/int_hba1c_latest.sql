{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Latest valid HbA1c measurement per person.
Uses the comprehensive int_hba1c_all model and filters to most recent valid HbA1c.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    hba1c_value,
    concept_code,
    concept_display,
    source_cluster_id,
    is_ifcc,
    is_dcct,
    result_unit_display,
    hba1c_result_display,
    hba1c_category,
    indicates_diabetes,
    meets_qof_target,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_hba1c_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_hba1c

WHERE is_valid_hba1c = TRUE
