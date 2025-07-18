{{
    config(
        materialized='table',
        tags=['data_quality', 'hba1c'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

-- HbA1c Data Quality Issues
-- Identifies HbA1c measurements that are flagged as invalid in int_hba1c_all
-- This table captures HbA1c values that are filtered out from the main HbA1c analysis tables
--
-- Valid HbA1c Ranges (maintains current clinically validated ranges):
-- - IFCC: 20-200 mmol/mol (international standard)
-- - DCCT: 3-20% (older percentage format)
-- Rationale:
-- - IFCC lower limit 20: Catches data entry errors while allowing very low HbA1c
-- - IFCC upper limit 200: Catches data entry errors while allowing extreme diabetic cases
-- - DCCT lower limit 3: Catches data entry errors while allowing very low HbA1c
-- - DCCT upper limit 20: Catches data entry errors while allowing extreme diabetic cases
-- - Dual scale complexity requires careful validation of measurement type detection

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    hba1c_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    is_ifcc,
    is_dcct,
    original_result_value,
    hba1c_result_display,
    hba1c_category,
    
    -- DQ Flags based on current validated ranges
    CASE 
        WHEN (is_ifcc AND (hba1c_value < 20 OR hba1c_value > 200)) OR
             (is_dcct AND (hba1c_value < 3 OR hba1c_value > 20)) THEN TRUE 
        ELSE FALSE 
    END AS is_hba1c_out_of_range,
    
    CASE 
        WHEN clinical_effective_date IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_date_missing,
    
    CASE 
        WHEN clinical_effective_date > CURRENT_DATE THEN TRUE 
        ELSE FALSE 
    END AS is_future_date,
    
    CASE 
        WHEN (is_ifcc AND hba1c_value > 150) OR
             (is_dcct AND hba1c_value > 15) THEN TRUE 
        ELSE FALSE 
    END AS is_extreme_outlier,
    
    CASE 
        WHEN (is_ifcc = FALSE AND is_dcct = FALSE) OR
             (is_ifcc = TRUE AND is_dcct = TRUE) THEN TRUE 
        ELSE FALSE 
    END AS is_measurement_type_ambiguous,
    
    -- Enhanced HbA1c category with DQ context
    CASE
        WHEN is_ifcc AND hba1c_value < 20 THEN 'IFCC Below Valid Range (< 20)'
        WHEN is_ifcc AND hba1c_value > 200 THEN 'IFCC Above Valid Range (> 200)'
        WHEN is_dcct AND hba1c_value < 3 THEN 'DCCT Below Valid Range (< 3)'
        WHEN is_dcct AND hba1c_value > 20 THEN 'DCCT Above Valid Range (> 20)'
        WHEN is_ifcc AND hba1c_value > 150 THEN 'IFCC Extremely High (> 150)'
        WHEN is_dcct AND hba1c_value > 15 THEN 'DCCT Extremely High (> 15)'
        WHEN (is_ifcc = FALSE AND is_dcct = FALSE) THEN 'Measurement Type Unknown'
        WHEN (is_ifcc = TRUE AND is_dcct = TRUE) THEN 'Measurement Type Ambiguous'
        ELSE hba1c_category
    END AS hba1c_category_with_issues

FROM {{ ref('int_hba1c_all') }}

-- Only include observations with DQ issues
WHERE (is_ifcc AND (hba1c_value < 20 OR hba1c_value > 200))  -- IFCC out of range
   OR (is_dcct AND (hba1c_value < 3 OR hba1c_value > 20))    -- DCCT out of range
   OR clinical_effective_date IS NULL                        -- Missing date
   OR clinical_effective_date > CURRENT_DATE                 -- Future date
   OR (is_ifcc = FALSE AND is_dcct = FALSE)                  -- Unknown measurement type
   OR (is_ifcc = TRUE AND is_dcct = TRUE)                    -- Ambiguous measurement type
   OR is_valid_hba1c = FALSE                                 -- Invalid per original logic

ORDER BY person_id, clinical_effective_date DESC