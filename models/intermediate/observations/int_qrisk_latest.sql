{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Latest valid QRISK cardiovascular risk score per person.
Uses the comprehensive int_qrisk_all model and filters to most recent valid QRISK.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    qrisk_score,
    qrisk_type,
    concept_code,
    concept_display,
    source_cluster_id,
    cvd_risk_category,
    is_high_cvd_risk,
    is_very_high_cvd_risk,
    warrants_statin_consideration,
    original_result_value

FROM (
    {{ get_latest_events(
        ref('int_qrisk_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_qrisk

WHERE is_valid_qrisk = TRUE
