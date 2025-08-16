-- Test that condition prevalence rates are within reasonable bounds
-- This helps catch data quality issues or calculation errors

WITH prevalence_rates AS (
    SELECT 
        analysis_month,
        COUNT(DISTINCT person_id) as total_population,
        
        -- Calculate prevalence for major conditions
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) / 
              NULLIF(COUNT(DISTINCT person_id), 0), 2) as diabetes_prevalence_pct,
        
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN has_htn THEN person_id END) / 
              NULLIF(COUNT(DISTINCT person_id), 0), 2) as hypertension_prevalence_pct,
        
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN has_ckd THEN person_id END) / 
              NULLIF(COUNT(DISTINCT person_id), 0), 2) as ckd_prevalence_pct,
        
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN has_copd THEN person_id END) / 
              NULLIF(COUNT(DISTINCT person_id), 0), 2) as copd_prevalence_pct,
        
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN has_af THEN person_id END) / 
              NULLIF(COUNT(DISTINCT person_id), 0), 2) as af_prevalence_pct,
        
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN has_dep THEN person_id END) / 
              NULLIF(COUNT(DISTINCT person_id), 0), 2) as depression_prevalence_pct
        
    FROM {{ ref('person_month_analysis_base') }}
    WHERE analysis_month >= DATEADD('month', -12, CURRENT_DATE)  -- Last 12 months
    GROUP BY analysis_month
),

issues AS (
    -- Check diabetes prevalence (typically 5-15% in UK)
    SELECT 
        analysis_month,
        'Diabetes prevalence out of bounds' as issue_type,
        diabetes_prevalence_pct::VARCHAR || '%' as actual_value,
        '5-20%' as expected_range
    FROM prevalence_rates
    WHERE diabetes_prevalence_pct NOT BETWEEN 0 AND 25
    
    UNION ALL
    
    -- Check hypertension prevalence (typically 15-35% in UK)
    SELECT 
        analysis_month,
        'Hypertension prevalence out of bounds' as issue_type,
        hypertension_prevalence_pct::VARCHAR || '%' as actual_value,
        '10-40%' as expected_range
    FROM prevalence_rates
    WHERE hypertension_prevalence_pct NOT BETWEEN 0 AND 45
    
    UNION ALL
    
    -- Check CKD prevalence (typically 5-15% in UK)
    SELECT 
        analysis_month,
        'CKD prevalence out of bounds' as issue_type,
        ckd_prevalence_pct::VARCHAR || '%' as actual_value,
        '3-20%' as expected_range
    FROM prevalence_rates
    WHERE ckd_prevalence_pct NOT BETWEEN 0 AND 25
    
    UNION ALL
    
    -- Check COPD prevalence (typically 2-8% in UK)
    SELECT 
        analysis_month,
        'COPD prevalence out of bounds' as issue_type,
        copd_prevalence_pct::VARCHAR || '%' as actual_value,
        '1-12%' as expected_range
    FROM prevalence_rates
    WHERE copd_prevalence_pct NOT BETWEEN 0 AND 15
    
    UNION ALL
    
    -- Check AF prevalence (typically 2-5% in UK)
    SELECT 
        analysis_month,
        'AF prevalence out of bounds' as issue_type,
        af_prevalence_pct::VARCHAR || '%' as actual_value,
        '1-10%' as expected_range
    FROM prevalence_rates
    WHERE af_prevalence_pct NOT BETWEEN 0 AND 12
    
    UNION ALL
    
    -- Check depression prevalence (typically 10-20% in UK)
    SELECT 
        analysis_month,
        'Depression prevalence out of bounds' as issue_type,
        depression_prevalence_pct::VARCHAR || '%' as actual_value,
        '5-30%' as expected_range
    FROM prevalence_rates
    WHERE depression_prevalence_pct NOT BETWEEN 0 AND 35
)

SELECT * FROM issues
ORDER BY analysis_month DESC, issue_type