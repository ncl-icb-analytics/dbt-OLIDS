{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid BMI measurement per person with ethnicity-adjusted categorisation.
Uses the int_bmi_all model and filters to most recent valid BMI.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    bmi_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    result_value,
    bmi_source,

    -- Ethnicity information
    requires_lower_bmi_thresholds,
    cardiometabolic_risk_ethnicity_group,

    -- Validation
    is_valid_bmi,

    -- BMI categorisation (ethnicity-adjusted)
    bmi_category,

    -- BMI risk sort key (ethnicity-adjusted)
    bmi_risk_sort_key

FROM {{ ref('int_bmi_all') }}
WHERE is_valid_bmi = TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1
