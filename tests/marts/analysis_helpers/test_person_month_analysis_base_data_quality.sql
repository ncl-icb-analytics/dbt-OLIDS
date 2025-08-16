-- Test data quality rules for person_month_analysis_base
-- Should return 0 rows if all data quality checks pass

WITH data_quality_checks AS (
    SELECT 
        person_id,
        analysis_month,
        age,
        sex,
        practice_name,
        financial_year,
        financial_quarter_number,
        month_number,
        total_active_conditions,
        total_new_episodes_this_month
    FROM {{ ref('person_month_analysis_base') }}
),

issues AS (
    -- Check: Sex should be valid
    SELECT 
        person_id,
        analysis_month,
        'Invalid sex value' as issue_type,
        sex as issue_value
    FROM data_quality_checks
    WHERE sex NOT IN ('Male', 'Female', 'Unknown', 'Other')
        OR sex IS NULL
    
    UNION ALL
    
    -- Check: Practice should exist for active registrations
    SELECT 
        person_id,
        analysis_month,
        'Missing practice' as issue_type,
        'NULL' as issue_value
    FROM data_quality_checks
    WHERE practice_name IS NULL
    
    UNION ALL
    
    -- Check: Financial quarter should be 1-4
    SELECT 
        person_id,
        analysis_month,
        'Invalid financial quarter' as issue_type,
        financial_quarter_number::VARCHAR as issue_value
    FROM data_quality_checks
    WHERE financial_quarter_number NOT BETWEEN 1 AND 4
    
    UNION ALL
    
    -- Check: Month number should be 1-12
    SELECT 
        person_id,
        analysis_month,
        'Invalid month number' as issue_type,
        month_number::VARCHAR as issue_value
    FROM data_quality_checks
    WHERE month_number NOT BETWEEN 1 AND 12
    
    UNION ALL
    
    -- Check: No future months
    SELECT 
        person_id,
        analysis_month,
        'Future month' as issue_type,
        analysis_month::VARCHAR as issue_value
    FROM data_quality_checks
    WHERE analysis_month > CURRENT_DATE()
    
    UNION ALL
    
    -- Check: Counts should be non-negative
    SELECT 
        person_id,
        analysis_month,
        'Negative condition count' as issue_type,
        total_active_conditions::VARCHAR as issue_value
    FROM data_quality_checks
    WHERE total_active_conditions < 0
    
    UNION ALL
    
    -- Check: New episodes should not exceed total conditions
    SELECT 
        person_id,
        analysis_month,
        'New episodes exceed total conditions' as issue_type,
        total_new_episodes_this_month::VARCHAR || ' > ' || total_active_conditions::VARCHAR as issue_value
    FROM data_quality_checks
    WHERE total_new_episodes_this_month > total_active_conditions
)

SELECT * FROM issues
LIMIT 100  -- Limit output for readability