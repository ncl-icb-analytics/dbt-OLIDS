{{
    config(
        materialized='table',
        tags=['data_quality', 'bmi'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

-- BMI Data Quality Issues
-- Identifies BMI measurements that are out of valid range or have other data quality issues
-- This table captures BMI values that are filtered out from the main BMI analysis tables
--
-- Valid BMI Range: 10-150 (updated from legacy 5-400 range)
-- Rationale:
-- - Lower limit 10: Catches data entry errors while allowing severe malnutrition cases
-- - Upper limit 150: Catches data entry errors while allowing extreme obesity cases
-- - Clinical reality: BMI < 10 incompatible with life, BMI > 150 extremely rare
-- - Most clinical systems use validation ranges of 10-150 for BMI

WITH base_observations AS (
    -- Get ALL BMI observations, including those with issues
    -- Keep NULL dates and out-of-range values for DQ analysis
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date, -- Keep NULL dates
        obs.result_value,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        -- Cast to number for range validation
        TRY_CAST(obs.result_value AS NUMBER(10,2)) AS bmi_value
    FROM ({{ get_observations("'BMIVAL_COD'") }}) obs
    WHERE obs.result_value IS NOT NULL -- Still need a value to assess
    -- DO NOT filter dates or values here - we want to catch all issues
),

bmi_with_flags AS (
    -- Add data quality flags to identify issues
    SELECT
        observation_id,
        person_id,
        clinical_effective_date,
        result_value,
        result_unit_display,
        concept_code,
        concept_display,
        source_cluster_id,
        bmi_value,
        
        -- DQ Flag: BMI out of valid range (< 10 or > 150)
        -- Updated from legacy range (5-400) to more clinically appropriate limits
        -- Lower limit 10: Catches data entry errors while allowing severe malnutrition
        -- Upper limit 150: Catches data entry errors while allowing extreme obesity
        CASE
            WHEN bmi_value IS NOT NULL
                 AND (bmi_value < 10 OR bmi_value > 150)
            THEN TRUE
            ELSE FALSE
        END AS is_bmi_out_of_range,
        
        -- DQ Flag: Non-numeric BMI value
        CASE
            WHEN bmi_value IS NULL AND result_value IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_non_numeric_bmi,
        
        -- DQ Flag: Missing date
        CASE
            WHEN clinical_effective_date IS NULL
            THEN TRUE
            ELSE FALSE
        END AS is_date_missing,
        
        -- DQ Flag: Extreme outlier (additional flag for very severe outliers)
        -- Values below 8 or above 100 are extremely rare and warrant investigation
        CASE
            WHEN bmi_value IS NOT NULL
                 AND (bmi_value < 8 OR bmi_value > 100)
            THEN TRUE
            ELSE FALSE
        END AS is_extreme_outlier,
        
        -- Clinical categorisation for context (even for invalid values)
        CASE
            WHEN bmi_value IS NULL THEN 'Non-numeric'
            WHEN bmi_value < 10 THEN 'Below Valid Range (< 10)'
            WHEN bmi_value > 150 THEN 'Above Valid Range (> 150)'
            WHEN bmi_value < 8 THEN 'Extremely Low (< 8)'
            WHEN bmi_value > 100 THEN 'Extremely High (> 100)'
            WHEN bmi_value < 18.5 THEN 'Underweight'
            WHEN bmi_value < 25 THEN 'Normal'
            WHEN bmi_value < 30 THEN 'Overweight'
            WHEN bmi_value < 35 THEN 'Obese Class I'
            WHEN bmi_value < 40 THEN 'Obese Class II'
            ELSE 'Obese Class III'
        END AS bmi_category_with_issues
    FROM base_observations
)

-- Final output: Only BMI measurements with data quality issues
SELECT
    observation_id,
    person_id,
    clinical_effective_date,
    result_value,
    bmi_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    bmi_category_with_issues,
    is_bmi_out_of_range,
    is_non_numeric_bmi,
    is_date_missing,
    is_extreme_outlier,
    
    -- Summary flag: Has any DQ issue
    CASE
        WHEN is_bmi_out_of_range OR is_non_numeric_bmi OR is_date_missing
        THEN TRUE
        ELSE FALSE
    END AS has_dq_issue

FROM bmi_with_flags

-- Only include observations with at least one DQ issue
WHERE is_bmi_out_of_range = TRUE
   OR is_non_numeric_bmi = TRUE
   OR is_date_missing = TRUE

ORDER BY person_id, clinical_effective_date DESC