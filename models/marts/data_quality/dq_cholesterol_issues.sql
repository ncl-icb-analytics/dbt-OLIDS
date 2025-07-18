{{
    config(
        materialized='table',
        tags=['data_quality', 'cholesterol'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

-- Cholesterol Data Quality Issues
-- Identifies cholesterol measurements that are flagged as invalid in int_cholesterol_all
-- This table captures cholesterol values that are filtered out from the main cholesterol analysis tables
--
-- Valid Cholesterol Range: 0.5-20 mmol/L (maintains current clinically validated range)
-- Rationale:
-- - Lower limit 0.5: Catches data entry errors while allowing very low cholesterol from malnutrition/medications
-- - Upper limit 20: Catches data entry errors while allowing severe familial hypercholesterolaemia
-- - Clinical reality: Normal <5.0, high >5.0, very high >7.5, severe cases can reach 15-20 mmol/L
-- - Conservative approach to avoid false positives in extreme but legitimate cases

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    cholesterol_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    original_result_value,
    
    -- DQ Flags based on current validated ranges
    CASE 
        WHEN cholesterol_value < 0.5 OR cholesterol_value > 20 THEN TRUE 
        ELSE FALSE 
    END AS is_cholesterol_out_of_range,
    
    CASE 
        WHEN clinical_effective_date IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_date_missing,
    
    CASE 
        WHEN clinical_effective_date > CURRENT_DATE THEN TRUE 
        ELSE FALSE 
    END AS is_future_date,
    
    CASE 
        WHEN cholesterol_value > 15 THEN TRUE 
        ELSE FALSE 
    END AS is_extreme_outlier,
    
    -- Cholesterol category with valid ranges
    CASE
        WHEN cholesterol_value < 0.5 THEN 'Below Valid Range (< 0.5)'
        WHEN cholesterol_value > 20 THEN 'Above Valid Range (> 20)'
        WHEN cholesterol_value > 15 THEN 'Extremely High (> 15)'
        WHEN cholesterol_value < 5.0 THEN 'Desirable'
        WHEN cholesterol_value < 6.2 THEN 'Borderline High'
        WHEN cholesterol_value < 7.8 THEN 'High'
        ELSE 'Very High'
    END AS cholesterol_category_with_issues

FROM {{ ref('int_cholesterol_all') }}

-- Only include observations with DQ issues using current validated ranges
WHERE (cholesterol_value < 0.5 OR cholesterol_value > 20)  -- Out of valid range
   OR clinical_effective_date IS NULL                      -- Missing date
   OR clinical_effective_date > CURRENT_DATE              -- Future date
   OR is_valid_cholesterol = FALSE                         -- Invalid per original logic

ORDER BY person_id, clinical_effective_date DESC