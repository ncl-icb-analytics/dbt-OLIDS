{{
    config(
        materialized='table',
        tags=['utility', 'date_spine'],
        cluster_by=['month_start_date']
    )
}}

-- Date Spine Utility Table
-- Provides comprehensive date dimensions for temporal analysis
-- Creates a monthly spine going back 10 years with all date helper columns pre-calculated

WITH date_range AS (
    -- Generate monthly dates for 10 years (120 months)
    SELECT 
        DATE_TRUNC('month', DATEADD('month', seq4() - 120, CURRENT_DATE)) as month_start_date
    FROM table(generator(rowcount => 120))
    WHERE month_start_date <= DATE_TRUNC('month', CURRENT_DATE)
)

SELECT 
    -- Core date columns
    month_start_date,
    LAST_DAY(month_start_date) as month_end_date,
    
    -- Date components
    YEAR(month_start_date) as year_number,
    QUARTER(month_start_date) as quarter_number,
    MONTH(month_start_date) as month_number,
    MONTHNAME(month_start_date) as month_name,
    TO_CHAR(month_start_date, 'MON') as month_abbr,
    
    -- Calendar periods
    DATE_TRUNC('quarter', month_start_date) as quarter_start_date,
    LAST_DAY(DATEADD('month', 2, DATE_TRUNC('quarter', month_start_date))) as quarter_end_date,
    DATE_TRUNC('year', month_start_date) as year_start_date,
    LAST_DAY(DATEADD('month', 11, DATE_TRUNC('year', month_start_date))) as year_end_date,
    
    -- Display labels
    TO_CHAR(month_start_date, 'YYYY-MM') as year_month,
    TO_CHAR(month_start_date, 'MON YYYY') as month_year_label,
    TO_CHAR(month_start_date, 'MMMM YYYY') as month_year_full,
    YEAR(month_start_date) || '-Q' || QUARTER(month_start_date) as calendar_quarter_label,
    
    -- UK Financial Year (April to March)
    CASE 
        WHEN MONTH(month_start_date) >= 4 
        THEN YEAR(month_start_date)
        ELSE YEAR(month_start_date) - 1
    END as financial_year_start,
    CASE 
        WHEN MONTH(month_start_date) >= 4 
        THEN YEAR(month_start_date) + 1
        ELSE YEAR(month_start_date)
    END as financial_year_end,
    CASE 
        WHEN MONTH(month_start_date) >= 4 
        THEN YEAR(month_start_date) || '/' || RIGHT(YEAR(month_start_date) + 1, 2)
        ELSE (YEAR(month_start_date) - 1) || '/' || RIGHT(YEAR(month_start_date), 2)
    END as financial_year_label,
    CASE 
        WHEN MONTH(month_start_date) IN (4, 5, 6) THEN 1
        WHEN MONTH(month_start_date) IN (7, 8, 9) THEN 2
        WHEN MONTH(month_start_date) IN (10, 11, 12) THEN 3
        WHEN MONTH(month_start_date) IN (1, 2, 3) THEN 4
    END as financial_quarter_number,
    CASE 
        WHEN MONTH(month_start_date) IN (4, 5, 6) THEN 'Q1'
        WHEN MONTH(month_start_date) IN (7, 8, 9) THEN 'Q2'
        WHEN MONTH(month_start_date) IN (10, 11, 12) THEN 'Q3'
        WHEN MONTH(month_start_date) IN (1, 2, 3) THEN 'Q4'
    END as financial_quarter_label,
    -- Financial quarter with year
    CASE 
        WHEN MONTH(month_start_date) >= 4 
        THEN YEAR(month_start_date) || '/' || RIGHT(YEAR(month_start_date) + 1, 2)
        ELSE (YEAR(month_start_date) - 1) || '/' || RIGHT(YEAR(month_start_date), 2)
    END || '-' ||
    CASE 
        WHEN MONTH(month_start_date) IN (4, 5, 6) THEN 'Q1'
        WHEN MONTH(month_start_date) IN (7, 8, 9) THEN 'Q2'
        WHEN MONTH(month_start_date) IN (10, 11, 12) THEN 'Q3'
        WHEN MONTH(month_start_date) IN (1, 2, 3) THEN 'Q4'
    END as financial_quarter_full_label,
    
    -- Financial period dates
    CASE 
        WHEN MONTH(month_start_date) >= 4 
        THEN DATE_FROM_PARTS(YEAR(month_start_date), 4, 1)
        ELSE DATE_FROM_PARTS(YEAR(month_start_date) - 1, 4, 1)
    END as financial_year_start_date,
    CASE 
        WHEN MONTH(month_start_date) >= 4 
        THEN DATE_FROM_PARTS(YEAR(month_start_date) + 1, 3, 31)
        ELSE DATE_FROM_PARTS(YEAR(month_start_date), 3, 31)
    END as financial_year_end_date,
    
    -- Relative date flags (calculated from current date)
    CASE WHEN month_start_date = DATE_TRUNC('month', CURRENT_DATE) 
         THEN TRUE ELSE FALSE END as is_current_month,
    CASE WHEN month_start_date = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE))
         THEN TRUE ELSE FALSE END as is_previous_month,
    CASE WHEN month_start_date >= DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE))
         THEN TRUE ELSE FALSE END as is_last_1_month,
    CASE WHEN month_start_date >= DATE_TRUNC('month', DATEADD('month', -3, CURRENT_DATE))
         THEN TRUE ELSE FALSE END as is_last_3_months,
    CASE WHEN month_start_date >= DATE_TRUNC('month', DATEADD('month', -6, CURRENT_DATE))
         THEN TRUE ELSE FALSE END as is_last_6_months,
    CASE WHEN month_start_date >= DATE_TRUNC('month', DATEADD('month', -12, CURRENT_DATE))
         THEN TRUE ELSE FALSE END as is_last_12_months,
    CASE WHEN month_start_date >= DATE_TRUNC('month', DATEADD('month', -24, CURRENT_DATE))
         THEN TRUE ELSE FALSE END as is_last_24_months,
    CASE WHEN month_start_date >= DATE_TRUNC('month', DATEADD('month', -36, CURRENT_DATE))
         THEN TRUE ELSE FALSE END as is_last_36_months,
    CASE WHEN month_start_date >= DATE_TRUNC('month', DATEADD('month', -60, CURRENT_DATE))
         THEN TRUE ELSE FALSE END as is_last_60_months,
    
    -- Months from current (for calculations)
    DATEDIFF('month', month_start_date, DATE_TRUNC('month', CURRENT_DATE)) as months_ago,
    
    -- Relative quarter flags
    CASE WHEN DATE_TRUNC('quarter', month_start_date) = DATE_TRUNC('quarter', CURRENT_DATE)
         THEN TRUE ELSE FALSE END as is_current_quarter,
    CASE WHEN DATE_TRUNC('quarter', month_start_date) = DATE_TRUNC('quarter', DATEADD('month', -3, CURRENT_DATE))
         THEN TRUE ELSE FALSE END as is_previous_quarter,
    
    -- Relative year flags
    CASE WHEN YEAR(month_start_date) = YEAR(CURRENT_DATE)
         THEN TRUE ELSE FALSE END as is_current_year,
    CASE WHEN YEAR(month_start_date) = YEAR(DATEADD('year', -1, CURRENT_DATE))
         THEN TRUE ELSE FALSE END as is_previous_year,
    
    -- Relative financial year flags
    CASE 
        WHEN MONTH(CURRENT_DATE) >= 4 AND MONTH(month_start_date) >= 4 
             AND YEAR(month_start_date) = YEAR(CURRENT_DATE)
        THEN TRUE
        WHEN MONTH(CURRENT_DATE) < 4 AND 
             ((MONTH(month_start_date) < 4 AND YEAR(month_start_date) = YEAR(CURRENT_DATE))
              OR (MONTH(month_start_date) >= 4 AND YEAR(month_start_date) = YEAR(CURRENT_DATE) - 1))
        THEN TRUE
        ELSE FALSE 
    END as is_current_financial_year,
    
    -- Sorting helpers
    month_start_date as sort_date,
    YEAR(month_start_date) * 100 + MONTH(month_start_date) as year_month_int,
    
    -- Metadata
    CURRENT_DATE as date_spine_created_date,
    CURRENT_TIMESTAMP as date_spine_created_timestamp

FROM date_range
ORDER BY month_start_date