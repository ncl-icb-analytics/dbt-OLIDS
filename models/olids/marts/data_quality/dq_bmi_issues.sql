{{
    config(
        materialized='table',
        tags=['data_quality', 'bmi'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

-- BMI Data Quality Issues
-- Identifies BMI measurements that are flagged as invalid in int_bmi_all
-- This table captures BMI values that are filtered out from the main BMI analysis tables
--
-- Valid BMI Range: 10-150 (updated from legacy 5-400 range)
-- Rationale:
-- - Lower limit 10: Catches data entry errors while allowing severe malnutrition cases
-- - Upper limit 150: Catches data entry errors while allowing extreme obesity cases
-- - Clinical reality: BMI < 10 incompatible with life, BMI > 150 extremely rare
-- - Most clinical systems use validation ranges of 10-150 for BMI

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
    bmi_category,
    
    -- DQ Flags based on our updated ranges
    CASE 
        WHEN bmi_value < 10 OR bmi_value > 150 THEN TRUE 
        ELSE FALSE 
    END AS is_bmi_out_of_range,
    
    CASE 
        WHEN clinical_effective_date IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_date_missing,
    
    CASE 
        WHEN clinical_effective_date > CURRENT_DATE THEN TRUE 
        ELSE FALSE 
    END AS is_future_date,
    
    CASE 
        WHEN bmi_value < 8 OR bmi_value > 100 THEN TRUE 
        ELSE FALSE 
    END AS is_extreme_outlier,
    
    -- Updated BMI category with new ranges
    CASE
        WHEN bmi_value < 10 THEN 'Below Valid Range (< 10)'
        WHEN bmi_value > 150 THEN 'Above Valid Range (> 150)'
        WHEN bmi_value < 8 THEN 'Extremely Low (< 8)'
        WHEN bmi_value > 100 THEN 'Extremely High (> 100)'
        ELSE bmi_category
    END AS bmi_category_with_issues

FROM {{ ref('int_bmi_all') }}

-- Only include observations with DQ issues using our updated ranges
WHERE (bmi_value < 10 OR bmi_value > 150)  -- Out of valid range
   OR clinical_effective_date IS NULL      -- Missing date
   OR clinical_effective_date > CURRENT_DATE -- Future date
   OR is_valid_bmi = FALSE                 -- Invalid per original logic

ORDER BY person_id, clinical_effective_date DESC