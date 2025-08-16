-- Test comprehensive temporal integrity for age historical SCD2 table
-- This should return 0 rows if temporal logic is correct

WITH period_analysis AS (
    SELECT 
        person_id,
        effective_start_date,
        effective_end_date,
        is_current_period,
        age,
        age_band_5y,
        LAG(effective_end_date) OVER (
            PARTITION BY person_id 
            ORDER BY effective_start_date
        ) as prev_end_date,
        LAG(age) OVER (
            PARTITION BY person_id 
            ORDER BY effective_start_date
        ) as prev_age
    FROM {{ ref('dim_person_age_historical') }}
),

issues AS (
    -- Check for gaps between periods
    SELECT 
        person_id,
        effective_start_date,
        effective_end_date,
        'Gap between age periods' as issue_type
    FROM period_analysis
    WHERE prev_end_date IS NOT NULL 
        AND effective_start_date > prev_end_date
    
    UNION ALL
    
    -- Check for overlapping periods  
    SELECT 
        person_id,
        effective_start_date,
        effective_end_date,
        'Overlapping age periods' as issue_type
    FROM period_analysis
    WHERE prev_end_date IS NOT NULL 
        AND effective_start_date < prev_end_date
    
    UNION ALL
    
    -- Check for multiple current periods
    SELECT 
        person_id,
        effective_start_date,
        effective_end_date,
        'Multiple current age periods' as issue_type
    FROM {{ ref('dim_person_age_historical') }}
    WHERE effective_end_date IS NULL
        AND person_id IN (
            SELECT person_id 
            FROM {{ ref('dim_person_age_historical') }}
            WHERE effective_end_date IS NULL
            GROUP BY person_id
            HAVING COUNT(*) > 1
        )
    
    UNION ALL
    
    -- Check that age progression is logical (not decreasing)
    SELECT 
        person_id,
        effective_start_date,
        effective_end_date,
        'Age decreased between periods' as issue_type
    FROM period_analysis
    WHERE prev_age IS NOT NULL 
        AND age < prev_age
)

SELECT * FROM issues