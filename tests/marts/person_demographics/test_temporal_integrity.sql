-- Test comprehensive temporal integrity for SCD2 table
-- This should return 0 rows if temporal logic is correct

WITH period_analysis AS (
    SELECT 
        person_id,
        effective_start_date,
        effective_end_date,
        is_current_period,
        LAG(effective_end_date) OVER (
            PARTITION BY person_id 
            ORDER BY effective_start_date
        ) as prev_end_date,
        LEAD(effective_start_date) OVER (
            PARTITION BY person_id 
            ORDER BY effective_start_date  
        ) as next_start_date
    FROM {{ ref('dim_person_demographics_historical') }}
),

issues AS (
    -- Check for gaps between periods
    SELECT 
        person_id,
        effective_start_date,
        effective_end_date,
        'Gap between periods' as issue_type
    FROM period_analysis
    WHERE prev_end_date IS NOT NULL 
        AND effective_start_date > prev_end_date
    
    UNION ALL
    
    -- Check for overlapping periods  
    SELECT 
        person_id,
        effective_start_date,
        effective_end_date,
        'Overlapping periods' as issue_type
    FROM period_analysis
    WHERE prev_end_date IS NOT NULL 
        AND effective_start_date < prev_end_date
    
    UNION ALL
    
    -- Check for multiple current periods
    SELECT 
        person_id,
        effective_start_date,
        effective_end_date,
        'Multiple current periods' as issue_type
    FROM {{ ref('dim_person_demographics_historical') }}
    WHERE effective_end_date IS NULL
        AND person_id IN (
            SELECT person_id 
            FROM {{ ref('dim_person_demographics_historical') }}
            WHERE effective_end_date IS NULL
            GROUP BY person_id
            HAVING COUNT(*) > 1
        )
    
    UNION ALL
    
    -- Check for invalid date ranges
    SELECT 
        person_id,
        effective_start_date,
        effective_end_date,
        'Invalid date range' as issue_type
    FROM {{ ref('dim_person_demographics_historical') }}
    WHERE effective_end_date IS NOT NULL 
        AND effective_start_date >= effective_end_date
)

SELECT * FROM issues