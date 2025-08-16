-- Test logical consistency between condition flags and summary metrics
-- This should return 0 rows if logic is correct

WITH consistency_checks AS (
    SELECT 
        person_id,
        analysis_month,
        has_dm,
        has_htn,
        has_ckd,
        has_copd,
        has_ast,
        has_af,
        has_chd,
        has_smi,
        has_dep,
        total_active_conditions,
        has_any_condition,
        
        -- Count conditions manually
        (CASE WHEN has_dm THEN 1 ELSE 0 END +
         CASE WHEN has_htn THEN 1 ELSE 0 END +
         CASE WHEN has_ckd THEN 1 ELSE 0 END +
         CASE WHEN has_copd THEN 1 ELSE 0 END +
         CASE WHEN has_ast THEN 1 ELSE 0 END +
         CASE WHEN has_af THEN 1 ELSE 0 END +
         CASE WHEN has_chd THEN 1 ELSE 0 END +
         CASE WHEN has_smi THEN 1 ELSE 0 END +
         CASE WHEN has_dep THEN 1 ELSE 0 END) as manually_counted_conditions
         
    FROM {{ ref('person_month_analysis_base') }}
    WHERE analysis_month >= DATEADD('month', -3, CURRENT_DATE)  -- Recent data only
),

issues AS (
    -- Check: has_any_condition should be TRUE if total_active_conditions > 0
    SELECT 
        person_id,
        analysis_month,
        'has_any_condition FALSE but has conditions' as issue_type,
        total_active_conditions as expected_count,
        0 as actual_count
    FROM consistency_checks
    WHERE has_any_condition = FALSE 
        AND total_active_conditions > 0
    
    UNION ALL
    
    -- Check: has_any_condition should be FALSE if total_active_conditions = 0
    SELECT 
        person_id,
        analysis_month,
        'has_any_condition TRUE but no conditions' as issue_type,
        0 as expected_count,
        total_active_conditions as actual_count
    FROM consistency_checks
    WHERE has_any_condition = TRUE 
        AND total_active_conditions = 0
    
    UNION ALL
    
    -- Check: total_active_conditions should be >= manually counted major conditions
    SELECT 
        person_id,
        analysis_month,
        'total_active_conditions less than major conditions' as issue_type,
        manually_counted_conditions as expected_count,
        total_active_conditions as actual_count
    FROM consistency_checks
    WHERE total_active_conditions < manually_counted_conditions
)

SELECT * FROM issues