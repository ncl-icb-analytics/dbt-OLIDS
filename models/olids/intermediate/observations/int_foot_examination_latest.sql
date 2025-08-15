{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest foot examination record per person.
Provides most recent foot check status including completion and risk assessment.
*/

SELECT
    person_id,
    clinical_effective_date,
    is_unsuitable,
    is_declined,
    left_foot_checked,
    right_foot_checked,
    both_feet_checked,
    left_foot_absent,
    right_foot_absent,
    left_foot_amputated,
    right_foot_amputated,
    left_foot_risk_level,
    right_foot_risk_level,
    townson_scale_level,
    all_concept_codes,
    all_concept_displays,
    all_source_cluster_ids,
    examination_status

FROM (
    {{ get_latest_events(
        ref('int_foot_examination_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_foot_examination
