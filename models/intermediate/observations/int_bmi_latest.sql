{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest valid BMI measurement per person.
Uses the int_bmi_all model and filters to most recent valid BMI and adds a sort key for BMI risk.
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
    
    -- BMI risk sort key (higher number = higher risk)
    CASE bmi_category
        WHEN 'Underweight' THEN 2  -- Health risk
        WHEN 'Normal' THEN 1  -- Baseline/lowest risk
        WHEN 'Overweight' THEN 3  -- Moderate risk
        WHEN 'Obese Class I' THEN 4  -- High risk
        WHEN 'Obese Class II' THEN 5  -- Higher risk
        WHEN 'Obese Class III' THEN 6  -- Highest risk
        ELSE 0  -- Unknown
    END AS bmi_risk_sort_key

FROM (
    {{ get_latest_events(
        ref('int_bmi_all'),
        partition_by=['person_id'],
        order_by=['clinical_effective_date']
    ) }}
)
WHERE is_valid_bmi = TRUE
