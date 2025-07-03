{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Latest Foot Examination Observations - Most recent foot examination status per person.

Clinical Purpose:
• Provides current diabetic foot examination status for diabetes care monitoring
• Supports foot risk assessment and QOF diabetes indicator reporting
• Enables current foot health status evaluation and care planning

Data Granularity:
• One row per person with their most recent foot examination
• Includes all patients with foot examination records
• Comprehensive examination status and risk assessment data

Key Features:
• Latest foot examination identification with completion status
• Current foot risk level assessment for clinical decision-making
• Bilateral foot status with anatomical considerations
• Complete examination metadata for clinical context'"
        ]
    )
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
