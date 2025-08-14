{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid BMI measurement per person.
Uses the int_bmi_all model and filters to most recent valid BMI.
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
    is_valid_bmi,
    bmi_source,
    bmi_risk_sort_key

FROM {{ ref('int_bmi_all') }}
WHERE is_valid_bmi = TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1
