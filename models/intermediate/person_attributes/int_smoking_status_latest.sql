{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Smoking Status Latest - Most recent smoking status per person for current clinical decision support and QOF reporting.

Clinical Purpose:
• Current smoking status determination for clinical decision support
• QOF smoking indicator extraction and cardiovascular risk assessment
• Real-time lifestyle intervention eligibility and patient safety protocols
• Smoking cessation programme targeting and health improvement tracking

Data Granularity:
• One row per person representing most recent smoking status observation
• Uses QOF cluster prioritisation for specific status codes
• Includes all persons regardless of status for comprehensive QOF reporting

Key Features:
• Latest smoking status with specific code prioritisation over general codes
• Current clinical decision support for CVD risk and intervention planning
• QOF-compliant smoking status determination for reporting
• Essential for real-time lifestyle intervention and patient safety protocols'"
        ]
    )
}}

/*
Latest smoking status per person based on most recent smoking-related observation.
Uses QOF definitions and prioritises specific status codes over general smoking codes.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    code_description,
    source_cluster_id,
    is_smoker_code,
    is_ex_smoker_code,
    is_never_smoked_code,
    smoking_status,
    is_current_smoker,
    is_ex_smoker

FROM (
    {{ get_latest_events(
        ref('int_smoking_status_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_smoking_status
