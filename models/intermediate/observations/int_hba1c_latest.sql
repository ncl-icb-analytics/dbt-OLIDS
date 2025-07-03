{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Latest HbA1c Observations - Most recent valid HbA1c measurement per person.

Clinical Purpose:
• Provides current glycaemic control status for diabetes management
• Supports diabetes care decision-making and QOF reporting
• Enables current diabetes control assessment and treatment planning

Data Granularity:
• One row per person with their most recent valid HbA1c
• Includes only patients with valid HbA1c measurements
• Supports both IFCC and DCCT measurement formats

Key Features:
• Latest valid HbA1c identification with measurement type preservation
• Clinical diabetes control categorisation for care pathway management
• QOF target achievement flags for quality indicator reporting
• Diabetes diagnostic indicators for clinical decision support'"
        ]
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
